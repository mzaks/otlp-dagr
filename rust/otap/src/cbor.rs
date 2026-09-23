//! The CBOR subset OTAP uses for a nested AnyValue (a kvlist or an array) in an attribute's
//! or a log body's `ser` column: text keys, integers, float64, bool, byte strings, null for
//! an empty value, no tags. Both decoders accept definite-length maps and arrays (Go's
//! fxamacker decoder into `interface{}`, Rust's ciborium). A kvlist's key order does NOT
//! survive Go's decoder (it lands in a Go map), whatever is written here.

pub fn head(out: &mut Vec<u8>, major: u8, n: u64) {
    let m = major << 5;
    match n {
        0..=23 => out.push(m | n as u8),
        24..=0xff => out.extend_from_slice(&[m | 24, n as u8]),
        0x100..=0xffff => {
            out.push(m | 25);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(m | 26);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
        _ => {
            out.push(m | 27);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
}

pub fn int(out: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        head(out, 0, v as u64);
    } else {
        head(out, 1, !(v as u64)); // -1 - v
    }
}

pub fn text(out: &mut Vec<u8>, s: &str) {
    head(out, 3, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

pub fn bytes(out: &mut Vec<u8>, b: &[u8]) {
    head(out, 2, b.len() as u64);
    out.extend_from_slice(b);
}

pub fn double(out: &mut Vec<u8>, v: f64) {
    out.push(0xfb);
    out.extend_from_slice(&v.to_bits().to_be_bytes());
}

pub fn boolean(out: &mut Vec<u8>, v: bool) {
    out.push(if v { 0xf5 } else { 0xf4 });
}

pub fn null(out: &mut Vec<u8>) {
    out.push(0xf6);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc8949_examples() {
        let enc = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut o = Vec::new();
            f(&mut o);
            o
        };
        assert_eq!(enc(&|o| int(o, 0)), [0x00]);
        assert_eq!(enc(&|o| int(o, 23)), [0x17]);
        assert_eq!(enc(&|o| int(o, 24)), [0x18, 0x18]);
        assert_eq!(enc(&|o| int(o, 1000)), [0x19, 0x03, 0xe8]);
        assert_eq!(enc(&|o| int(o, -1)), [0x20]);
        assert_eq!(enc(&|o| int(o, -1000)), [0x39, 0x03, 0xe7]);
        assert_eq!(enc(&|o| int(o, i64::MIN)), [0x3b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(enc(&|o| text(o, "a")), [0x61, 0x61]);
        assert_eq!(enc(&|o| bytes(o, &[1, 2])), [0x42, 1, 2]);
        assert_eq!(enc(&|o| double(o, 1.1)), [0xfb, 0x3f, 0xf1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a]);
        assert_eq!(enc(&|o| head(o, 5, 2)), [0xa2]);
    }
}
