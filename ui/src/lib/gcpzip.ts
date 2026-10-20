/** Minimal STORE-only zip writer for the inline source editor. */

export type ZipFile = { name: string; data: Uint8Array };

const SIG_LOCAL = 0x04034b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_END = 0x06054b50;
const VERSION = 20;
const UTF8_FLAG = 0x0800;
// 1980-01-01
const DOS_DATE = 0x0021;
const CRC_POLY = 0xedb88320;
const BYTE = 256;

const TABLE = Array.from({ length: BYTE }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? CRC_POLY ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

export function crc32(data: Uint8Array): number {
  let c = 0xffffffff;
  for (const b of data) c = TABLE[(c ^ b) & 0xff]! ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function put(parts: number[][], ...fields: [number, number][]) {
  const out: number[] = [];
  for (const [value, bytes] of fields) {
    for (let i = 0; i < bytes; i++) out.push((value >>> (8 * i)) & 0xff);
  }
  parts.push(out);
}

/** Builds an uncompressed zip archive holding files. */
export function zipStore(files: ZipFile[]): Uint8Array {
  const enc = new TextEncoder();
  const chunks: Uint8Array[] = [];
  const central: number[][] = [];
  const names: Uint8Array[] = [];
  let offset = 0;

  for (const f of files) {
    const name = enc.encode(f.name);
    const crc = crc32(f.data);
    const head: number[][] = [];
    put(
      head,
      [SIG_LOCAL, 4],
      [VERSION, 2],
      [UTF8_FLAG, 2],
      [0, 2],
      [0, 2],
      [DOS_DATE, 2],
      [crc, 4],
      [f.data.length, 4],
      [f.data.length, 4],
      [name.length, 2],
      [0, 2],
    );
    chunks.push(Uint8Array.from(head[0]!), name, f.data);
    put(
      central,
      [SIG_CENTRAL, 4],
      [VERSION, 2],
      [VERSION, 2],
      [UTF8_FLAG, 2],
      [0, 2],
      [0, 2],
      [DOS_DATE, 2],
      [crc, 4],
      [f.data.length, 4],
      [f.data.length, 4],
      [name.length, 2],
      [0, 2],
      [0, 2],
      [0, 2],
      [0, 2],
      [0, 4],
      [offset, 4],
    );
    names.push(name);
    offset += head[0]!.length + name.length + f.data.length;
  }

  let centralSize = 0;
  central.forEach((c, i) => {
    chunks.push(Uint8Array.from(c), names[i]!);
    centralSize += c.length + names[i]!.length;
  });

  const end: number[][] = [];
  put(
    end,
    [SIG_END, 4],
    [0, 2],
    [0, 2],
    [files.length, 2],
    [files.length, 2],
    [centralSize, 4],
    [offset, 4],
    [0, 2],
  );
  chunks.push(Uint8Array.from(end[0]!));

  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
  let pos = 0;
  for (const c of chunks) {
    out.set(c, pos);
    pos += c.length;
  }
  return out;
}
