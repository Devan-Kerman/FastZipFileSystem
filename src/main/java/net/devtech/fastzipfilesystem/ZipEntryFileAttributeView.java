package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

final class ZipEntryFileAttributeView implements BasicFileAttributeView {
	final FastZipPath path;
	final FastZipEntry entry;
	
	ZipEntryFileAttributeView(FastZipPath path) {
		this.path = path;
		this.entry = path.getAnyEntry();
		
	}
	
	@Override
	public String name() {
		return "basic";
	}
	
	@Override
	public BasicFileAttributes readAttributes() throws IOException {
		return new Basic();
	}
	
	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
		long max = 0;
		if(lastModifiedTime != null) max = Math.max(max, lastModifiedTime.toMillis());
		if(lastAccessTime != null) max = Math.max(max, lastAccessTime.toMillis());
		if(createTime != null) max = Math.max(max, createTime.toMillis());
		this.path.getAnyEntry().lastMod = max;
	}
	
	final class Basic implements BasicFileAttributes {
		@Override
		public FileTime lastModifiedTime() {
			return FileTime.fromMillis(entry.lastMod);
		}
		
		@Override
		public FileTime lastAccessTime() {
			return FileTime.fromMillis(entry.lastMod);
		}
		
		@Override
		public FileTime creationTime() {
			return FileTime.fromMillis(entry.lastMod);
		}
		
		@Override
		public boolean isRegularFile() {
			return !isDirectory();
		}
		
		@Override
		public boolean isDirectory() {
			return path.isDirectory();
		}
		
		@Override
		public boolean isSymbolicLink() {
			return false;
		}
		
		@Override
		public boolean isOther() {
			return false;
		}
		
		@Override
		public long size() {
			return entry.contents.uncompressedSize;
		}
		
		@Override
		public Object fileKey() {
			return path;
		}
	}
}
