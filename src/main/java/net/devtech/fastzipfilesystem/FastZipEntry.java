package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipException;

class FastZipEntry {
	private static final ConcurrentLinkedDeque<Deflater> DEFLATERS = new ConcurrentLinkedDeque<>();
	static final int METHOD_STORED = 0;
	static final int METHOD_DEFLATED = 8;
	static final int METHOD_DEFLATED64 = 9;
	static final int METHOD_BZIP2 = 12;
	static final int METHOD_LZMA = 14;
	static final int METHOD_LZ77 = 19;
	static final int METHOD_AES = 99;
	
	ByteBuffer name;
	ByteBuffer comment;
	
	ZipContents contents;
	int externalFileAttributes;
	long lastMod;
	
	boolean valid = true;
	
	final Set<ByteBuffer> children = new ConcurrentSkipListSet<>();
	
	static final class ZipContents {
		BigByteBuffer compressedData;
		Object uncompressedData;
		long compressedSize, uncompressedSize;
		long compressedOffset, uncompressedOffset;
		short compressionMethod;
		int crc32;
		
		ZipContents() {
			// TODO make default as deflated
		}
		
		ZipContents(BigByteBuffer uncompressed) {
			this.uncompressedData = uncompressed;
			this.compressedOffset = 0;
			this.compressedSize = uncompressed.size;
			this.compressionMethod = METHOD_DEFLATED;
		}
		
		public BigByteBuffer compress() throws IOException {
			if(this.compressedData != null) {
				return this.compressedData;
			}
			
			BigByteBuffer uncompressed = null;
			if(this.uncompressedData instanceof SoftReference<?> r) {
				uncompressed = (BigByteBuffer) r.get();
			} else if(this.uncompressedData != null) {
				uncompressed = (BigByteBuffer) this.uncompressedData;
			}
			
			Objects.requireNonNull(uncompressed, "no uncompressed data!");
			
			if(this.compressionMethod == METHOD_STORED) {
				CRC32 crc = new CRC32();
				uncompressed.segmentedInsert(crc::update, this.uncompressedOffset, this.uncompressedSize);
				this.compressedSize = this.uncompressedSize;
				this.compressedOffset = this.uncompressedOffset;
				this.compressedData = uncompressed;
				this.crc32 = (int) crc.getValue();
				return uncompressed;
			}
			
			Deflater pop = DEFLATERS.pollFirst();
			if(pop == null) {
				pop = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
			}
			try {
				BigByteBuffer compressedData = new BigByteBuffer();
				ByteBuffer temp = ByteBuffer.allocate(1024);
				Deflater deflater = pop;
				CRC32 crc32 = new CRC32();
				uncompressed.segmentedInsert(buffer -> {
					deflater.setInput(buffer);
					buffer.position(0);
					crc32.update(buffer);

					while(!deflater.finished()) {
						if(deflater.needsInput()) {
							break;
						}

						temp.clear();
						deflater.deflate(temp);
						compressedData.append(buffer, compressedData.size);
					}

				}, this.uncompressedOffset, this.uncompressedSize);

				if(!deflater.finished()) {
					throw new ZipException("Reported Inflated size > Actual Inflated Size");
				}
				this.crc32 = (int) crc32.getValue();
				this.compressedOffset = 0;
				this.compressedSize = compressedData.size;
				this.compressedData = compressedData;
				this.uncompressedData = new SoftReference<>(uncompressed);
				return compressedData;
			} finally {
				pop.reset();
				DEFLATERS.push(pop);
			}
		}
		
		public BigByteBuffer decompress() throws IOException {
			BigByteBuffer buffer;
			if(this.uncompressedData instanceof SoftReference<?> r) {
				if((buffer = (BigByteBuffer) r.get()) != null) {
					return buffer;
				}
			} else if(this.uncompressedData != null) {
				return (BigByteBuffer) this.uncompressedData;
			}
			
			if(this.compressionMethod == METHOD_STORED) {
				this.uncompressedOffset = this.compressedOffset;
				this.uncompressedSize = this.compressedSize;
				this.uncompressedData = this.compressedData;
				return this.compressedData;
			} else if(this.compressionMethod == METHOD_DEFLATED) {
				BigByteBuffer data = new BigByteBuffer(this.uncompressedSize, this.compressedData, this.compressedOffset, this.compressedSize);
				this.uncompressedData = new SoftReference<>(data);
				this.uncompressedOffset = 0;
				return data;
			} else {
				throw new UnsupportedOperationException("Unsupported compression method " + switch(this.compressionMethod) {
					case METHOD_BZIP2 -> "bzip2";
					case METHOD_AES -> "aes";
					case METHOD_DEFLATED64 -> "deflated64";
					case METHOD_LZ77 -> "lz77";
					case METHOD_LZMA -> "lzma";
					default -> "<unknown " + this.compressionMethod + ">";
				});
			}
		}
	}
	
	FastZipEntry(ByteBuffer name) {
		this.name = name;
	}
	
	// todo remember on flush to set this to a soft reference
	// todo automatically flush with cleaner
	
	public void write(BigByteBuffer uncompressed) {
		this.contents = new ZipContents(uncompressed);
	}
	
	@Override
	public String toString() {
		return FastZipUtil.toStr(this.name) + "";
	}
}
