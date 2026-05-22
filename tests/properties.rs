use bytes::{Bytes, BytesMut};
use kafko::Record;
use proptest::prelude::*;

proptest! {
    #[test]
    fn record_roundtrips_any_input(
        ts in any::<i64>(),
        key in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..1024)),
        value in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let record = Record::new(
            ts,
            key.map(Bytes::from),
            Bytes::from(value),
        );
        let expected = record.clone();
        let expected_size = record.wire_size();

        let mut buf = BytesMut::new();
        record.encode(&mut buf);
        prop_assert_eq!(buf.len(), expected_size);

        let mut slice: &[u8] = &buf;
        let decoded = Record::decode(&mut slice).expect("decode after encode must succeed");
        prop_assert_eq!(decoded, expected);
        prop_assert!(slice.is_empty());
    }
}
