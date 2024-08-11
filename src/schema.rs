// @generated automatically by Diesel CLI.

diesel::table! {
    miners (id) {
        id -> Integer,
        #[max_length = 44]
        pubkey -> Varchar,
        balance -> Nullable<Bigint>,
        enabled -> Nullable<Bool>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}
