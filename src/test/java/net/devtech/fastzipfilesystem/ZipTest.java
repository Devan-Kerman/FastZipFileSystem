package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class ZipTest {
	public static void main(String[] args) throws IOException {
		Path read = Path.of("test.zip");
		Path src = Path.of("openlogic-openjdk-8u332-b09-windows-x64.zip");
		Files.deleteIfExists(read);
		Files.copy(src, read);
		//try(FileSystem fastZipFS = FileSystems.newFileSystem(read, Map.of())) {
		//	Path path = fastZipFS.getPath("/openlogic-openjdk-8u332-b09-windows-64/jre/lib/psfont.properties.ja");
		//	System.out.println(Files.readString(path));
		//} catch(IOException e) {
		//	throw new RuntimeException(e);
		//}
		//33942392
		//33942384
		FastZipFSProvider provider = new FastZipFSProvider();
		try(FastZipFS fastZipFS = new FastZipFS(provider, read, Map.of())) {
			Path path = fastZipFS.getPath("/openlogic-openjdk-8u332-b09-windows-64/jre/lib/psfont.properties.ja");
			System.out.println(Files.readString(path));
			Files.writeString(path, "hello there!"); // todo no effect?
		} catch(IOException e) {
			throw new RuntimeException(e);
		}

		try(FastZipFS fastZipFS = new FastZipFS(provider, read, Map.of())) {
			Path path = fastZipFS.getPath("/openlogic-openjdk-8u332-b09-windows-64/demo/README");
			System.out.println(Files.readString(path));
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
}
