// Gzips the built SPA in place (file -> file.gz). The Go embed serves the
// .gz bytes directly to gzip-capable clients and inflates for the rest.
import { readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync, constants } from "node:zlib";

const spaDir = fileURLToPath(new URL("../../dashboard/static/spa", import.meta.url));
const compressible = new Set([".js", ".css", ".html", ".json", ".svg", ".txt", ".map"]);

let before = 0;
let after = 0;

const walk = (dir) => {
  for (const name of readdirSync(dir)) {
    const file = join(dir, name);
    if (statSync(file).isDirectory()) {
      walk(file);
      continue;
    }
    if (!compressible.has(extname(name))) {
      continue;
    }
    const raw = readFileSync(file);
    const gz = gzipSync(raw, { level: constants.Z_BEST_COMPRESSION });
    writeFileSync(`${file}.gz`, gz);
    rmSync(file);
    before += raw.length;
    after += gz.length;
  }
};

walk(spaDir);
console.log(`compress-spa: ${before} -> ${after} bytes`);
