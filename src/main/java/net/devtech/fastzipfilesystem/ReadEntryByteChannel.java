package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.zip.ZipException;

class ReadEntryByteChannel implements SeekableByteChannel, ScatteringByteChannel {
	FastZipEntry.ZipContents building;
	long pos, size;
	
	ReadEntryByteChannel(FastZipEntry entry) throws ZipException {
		building(entry);
	}
	
	protected void building(FastZipEntry entry) {
		FastZipEntry.ZipContents contents = entry.contents;
		this.size = contents.uncompressedSize;
		this.building = contents;
	}
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		BigByteBuffer data = this.building.decompress();
		long offset = building.uncompressedOffset + pos;
		long l = data.segmentedInsert(dst::put, offset, Math.min(dst.remaining(), size - pos));
		this.pos += l;
		return (int) l;
	}
	
	@Override
	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
		BigByteBuffer data = this.building.decompress();
		long uncompressedOffset = building.uncompressedOffset;
		int[] idx = {0};
		return data.segmentedInsert0(buffer -> {
			while(buffer.hasRemaining()) {
				ByteBuffer output = dsts[idx[0]];
				int min = Math.min(output.remaining(), buffer.remaining());
				output.put(output.position(), buffer, buffer.position(), min);
				buffer.position(buffer.position()+min);
				output.position(output.position()+min);
				if(!output.hasRemaining()) {
					idx[0]++;
				}
			}
			return buffer.position();
		}, uncompressedOffset, size - pos);
	}
	
	@Override
	public long read(ByteBuffer[] dsts) throws IOException {
		return this.read(dsts, 0, dsts.length);
	}
	
	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new ReadOnlyBufferException();
	}
	
	@Override
	public long position() {
		return this.pos;
	}
	
	@Override
	public SeekableByteChannel position(long newPosition) {
		this.pos = newPosition;
		return this;
	}
	
	@Override
	public long size() {
		return this.size;
	}
	
	@Override
	public SeekableByteChannel truncate(long size) {
		this.size = size;
		return this;
	}
	
	@Override
	public boolean isOpen() {
		return true;
	}
	
	@Override
	public void close() throws IOException {
	
	}
	
}
