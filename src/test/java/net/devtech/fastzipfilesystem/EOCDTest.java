package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

public class EOCDTest {
	public static void main(String[] args) throws IOException {
		FastZipFSProvider provider = new FastZipFSProvider();
		Path of = Path.of("test.zip");
		try(FastZipFS fastZipFS = new FastZipFS(provider, of, Map.of()); FileSystem zipFs = FileSystems.newFileSystem(of)) {
			Path fastPath = fastZipFS.getPath("assets/toastcontrol/lang/en_us.json");
			Path path = zipFs.getPath("assets/toastcontrol/lang/en_us.json");
			Files.writeString(fastPath, "teytuiuiytrfdfghjufghytytrfghgyhtrtgfdbgghsdcccvst");
			System.out.println(Arrays.toString(Files.readAllBytes(fastPath)));
			System.out.println(Arrays.toString(Files.readAllBytes(path)));
			System.out.println();
			
			System.out.println(fastPath.getFileName());
			System.out.println(path.getFileName());
			System.out.println();
			
			System.out.println(fastPath.subpath(3, 4));
			System.out.println(path.subpath(3, 4));
			System.out.println();
			
			System.out.println(fastPath.subpath(0, 3));
			System.out.println(path.subpath(0, 3));
			System.out.println();
			
			Path fastDir = fastZipFS.getPath("assets/toastcontrol/lang/");
			Path dir = zipFs.getPath("assets/toastcontrol/lang/");
			
			
			System.out.println(fastDir.getFileName());
			System.out.println(dir.getFileName());
			System.out.println();
			
			System.out.println(fastDir.subpath(2, 3));
			System.out.println(dir.subpath(2, 3));
			System.out.println();
			
			System.out.println(fastDir.subpath(0, 3));
			System.out.println(dir.subpath(0, 3));
			System.out.println();
			
			System.out.println(fastDir);
			System.out.println(dir);
			System.out.println();
			
			System.out.println(fastDir.getFileName());
			System.out.println(dir.getFileName());
			System.out.println();
			
			System.out.println(fastPath.getFileName().getNameCount());
			System.out.println(path.getFileName().getNameCount());
			
			System.out.println(fastDir.endsWith(fastDir.getFileName()));
			System.out.println(path.endsWith(path.getFileName()));
			
			System.out.println(fastDir.endsWith(fastZipFS.getPath(".json")));
			System.out.println(path.endsWith(zipFs.getPath(".json")));
			
			System.out.println(fastDir.startsWith(fastDir.getParent()));
			System.out.println(dir.startsWith(dir.getParent()));
			
			System.out.println(fastDir.startsWith(fastZipFS.getPath("assetsgyibuonfiouwenfiwonfoiwiofnwoifwoifints_")));
			System.out.println(path.startsWith(zipFs.getPath("assetsgyibuonfiouwenfiwonfoiwiofnwoifwoifints_")));
			
			System.out.println(fastDir.relativize(fastDir));
			
			System.out.println(fastDir.resolve("test"));
			System.out.println(fastDir.resolve("test/"));
			System.out.println(fastDir.resolve(fastZipFS.getPath("test")));
			System.out.println(fastDir.resolve(fastDir.relativize(fastDir)));
			System.out.println(fastDir.toUri());
			
			System.out.println(fastDir.toRealPath());
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
}
