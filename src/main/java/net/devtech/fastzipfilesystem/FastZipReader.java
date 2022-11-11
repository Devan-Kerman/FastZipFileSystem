package net.devtech.fastzipfilesystem;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.ZipException;

class FastZipReader {
	public static final int LOC_HEADER = 0x04034B50;
	static final int FILE_ATTRIB_UNIX = 3;
	static final int CEN_HEADER = 0x02014b50;
	static final short ZIP64_EXT_INFO_HEADER = 0x0001;
	
	record CentralInformation(BigByteBuffer comment) {}
	
	static CentralInformation read(BigByteBuffer buffer, Consumer<FastZipEntry> reader, Runnable clearState) throws IOException {
		EOCD eocd = null;
		ByteBuffer entryBuf = ByteBuffer.allocate(46).order(ByteOrder.LITTLE_ENDIAN);
		List<Throwable> exceptions = new ArrayList<>();
		while((eocd = nextEOCDCandidate(buffer, eocd)) != null) {
			try {
				clearState.run();
				long start = eocd.headerStart;
				for(long i = 0; i < eocd.directories; i++) {
					buffer.read(start, entryBuf, 0, entryBuf.capacity());
					int header = entryBuf.getInt(0);
					if(header != CEN_HEADER) {
						throw new ZipException("Expected header " + Integer.toHexString(CEN_HEADER) + " found " + Integer.toHexString(header));
					}
					
					int nameLen = entryBuf.getShort(28) & 0xFFFF;
					ByteBuffer name = buffer.slice(start + 46, nameLen);
					FastZipEntry entry = new FastZipEntry(name);
					short method = entryBuf.getShort(10);
					entry.lastMod = FastZipUtil.dosToJavaTime(entryBuf.getShort(12), entryBuf.getShort(14));
					int crc32 = entryBuf.getInt(16);
					long compressedSize = entryBuf.getInt(20) & 0xFFFFFFFFL;
					long uncompressedSize = entryBuf.getInt(24) & 0xFFFFFFFFL;
					entry.externalFileAttributes = entryBuf.getInt(38);
					int extraLen = entryBuf.getShort(30) & 0xFFFF;
					long baseOffset = entryBuf.getInt(42) & 0xFFFFFFFFL;
					ByteBuffer localHeader = buffer.slice(baseOffset, 30);
					int signature = localHeader.getInt(0);
					if(signature != LOC_HEADER) {
						throw new ZipException("Expected header signature " + Integer.toHexString(LOC_HEADER) + " found " + Integer.toHexString(signature));
					}
					int locLen = ((localHeader.getShort(26) & 0xFFFF) + (localHeader.getShort(28) & 0xFFFF) + 30);
					long compressedOffset = baseOffset + locLen;
					
					int commentLen = entryBuf.getShort(32) & 0xFFFF;
					entry.comment = buffer.slice(start + 46 + nameLen + extraLen, commentLen);
					
					if(extraLen > 0) {
						ByteBuffer extra = buffer.slice(start + 46 + nameLen, extraLen);
						
						int extraOffset = 0;
						while(extraOffset < extraLen) {
							short aShort = extra.getShort(0);
							if(aShort == ZIP64_EXT_INFO_HEADER && eocd.zip64) {
								int size = extra.getShort(2) & 0xFFFF;
								if(uncompressedSize == 0xFFFFFFFFL && extraOffset < size) {
									uncompressedSize = extra.getLong(4 + extraOffset);
									extraOffset += 8;
								}
								
								if(compressedSize == 0xFFFFFFFFL && extraOffset < size) {
									uncompressedSize = extra.getLong(4 + extraOffset);
									extraOffset += 8;
								}
								
								if(compressedOffset == 0xFFFFFFFFL && extraOffset < size) {
									compressedOffset = extra.getLong(4 + extraOffset);
									extraOffset += 8;
								}
							} else {
								break;
							}
						}
					}
					
					FastZipEntry.ZipContents contents = new FastZipEntry.ZipContents();
					contents.compressedData = buffer;
					contents.compressedSize = compressedSize;
					contents.uncompressedSize = uncompressedSize;
					contents.compressedOffset = compressedOffset;
					contents.compressionMethod = method;
					contents.crc32 = crc32;
					entry.contents = contents;
					start += 46 + nameLen + extraLen + commentLen;
					reader.accept(entry);
				}
				BigByteBuffer comment = new BigByteBuffer(buffer, buffer.size - eocd.commentLength, eocd.commentLength);
				return new CentralInformation(comment);
			} catch(Throwable t) {
				exceptions.add(t);
			}
		}
		
		if(!exceptions.isEmpty()) {
			int min = Math.min(exceptions.size(), 3) - 1;
			for(int i = 0; i < min; i++) {
				exceptions.get(i).printStackTrace();
			}
			throw new IOException("Unable to read zip file!", exceptions.get(min));
		}
		throw new EOFException("Unable to find End of Central Directory Record!");
	}
	
	static final int EOCD_HEADER = 0x06054B50, EOCD64_HEADER = 0x06064B50;
	
	@SuppressWarnings("PointlessArithmeticExpression")
	static EOCD nextEOCDCandidate(BigByteBuffer copy, EOCD last) {
		long start = last == null ? copy.size : last.commentLength - 1;
		final int bufLen = 56, zip64EOCDLen = 56, zipEOCDLen = 22;
		ByteBuffer buf = ByteBuffer.allocate(bufLen).order(ByteOrder.LITTLE_ENDIAN);
		// converts relative eocd pos to buffer position because we are reading this backwards
		while(true) {
			long position = start - bufLen;
			if(position < 0) {
				return null;
			}
			copy.read(position, buf, 0, buf.capacity());
			int zipOff = bufLen - zipEOCDLen;
			int zip32Header = buf.getInt(zipOff + 0);
			
			// if the header is right, and the comment length is right, then the chance of reading this zip file incorrectly is
			//  approximately (1+n)/intmax where n is the size of the comment
			if(zip32Header == EOCD_HEADER && buf.getShort(zipOff + 20) == (copy.size - start)) { // todo check for zip64
				short disk = buf.getShort(zipOff + 4);
				short diskStart = buf.getShort(zipOff + 6);
				short cens = buf.getShort(zipOff + 8);
				short totalCens = buf.getShort(zipOff + 10);
				int size = buf.getInt(zipOff + 12);
				int offset = buf.getInt(zipOff + 16);
				if(disk != -1 || diskStart != -1 || cens != -1 || totalCens != -1 || size != -1 || offset != -1) {
					return new EOCD(offset & 0xFFFFFFFFL, start, totalCens, false);
				}
			}
			
			int zip64Header = buf.getInt(zip64EOCDLen - 56);
			if(zip64Header == EOCD64_HEADER && (buf.getLong(4) + 12) == ((copy.size - start) + 56)) {
				return new EOCD(buf.getLong(48), start, buf.getLong(32), true);
			}
			
			start--;
		}
	}
	
	record EOCD(long headerStart, long commentLength, long directories, boolean zip64) {}
}
