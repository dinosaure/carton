[@@@warning "-32"]

type bigstring = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

external get_int32 : bigstring -> int -> int32 = "%caml_bigstring_get32"
external get_int64 : bigstring -> int -> int64 = "%caml_bigstring_get64"
external get_int16 : bigstring -> int -> int = "%caml_bigstring_get16"
external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"
external swap16 : int -> int = "%bswap16"

let get_int16_be =
  if Sys.big_endian
  then fun buf off -> get_int16 buf off
  else fun buf off -> swap16 (get_int16 buf off)

let get_int64_be =
  if Sys.big_endian
  then fun buf off -> get_int64 buf off
  else fun buf off -> swap64 (get_int64 buf off)

external string_get_int16 : string -> int -> int = "%caml_string_get16"
external string_get_int64 : string -> int -> int64 = "%caml_string_get64"

let string_get_int16_be =
  if Sys.big_endian
  then fun buf off -> string_get_int16 buf off
  else fun buf off -> swap16 (string_get_int16 buf off)

let string_get_int64_be =
  if Sys.big_endian
  then fun buf off -> string_get_int64 buf off
  else fun buf off -> swap64 (string_get_int64 buf off)

let string_get_int8 s i =
  Char.code (String.get s i)

let get_int32_be =
  if Sys.big_endian
  then fun buf off -> get_int32 buf off
  else fun buf off -> swap32 (get_int32 buf off)

type 'uid idx =
  { mp : bigstring
  ; n : int
  ; uid_ln : int
  ; uid_rw : 'uid -> string
  ; uid_wr : string -> 'uid }
and sub =
  { off : int
  ; len : int }
and optint = Optint.t

let make
  : bigstring -> uid_ln:int -> uid_rw:('uid -> string) -> uid_wr:(string -> 'uid) -> 'uid idx
  = fun mp ~uid_ln ~uid_rw ~uid_wr ->
    let i = get_int32_be mp 0 in
    let v = get_int32_be mp 4 in
    let n = get_int32_be mp (8 + (255 * 4)) in

    if i <> 0xff744f63l then Fmt.invalid_arg "Invalid IDX file (header: %lx <> %lx)" i 0xf744f63l ;
    if v <> 0x2l then Fmt.invalid_arg "Invalid version of IDX file" ;
    { mp; n= Int32.to_int n; uid_ln; uid_rw; uid_wr; }

let pp_uid ppf x =
  let x : Digestif.SHA1.t = Obj.magic x in
  Digestif.SHA1.pp ppf x

let compare_bigstring idx a hash =
  let ps = ref 0 in
  let c1 = ref 0 in
  let c2 = ref 0 in

  let exception Equal in

  try
    while c1 := get_int16_be idx.mp (a.off + !ps) ;
          c2 := string_get_int16_be hash !ps ;
      !c1 == !c2
    do ps := !ps + 2 ; if !ps == idx.uid_ln then raise_notrace Equal done ;

    let res0 = !c1 - !c2 in
    let res1 = res0 asr 8 in
    if res1 == 0 then res0 else res1
  with Equal -> 0

let ( <-> ) a b = Int32.sub a b
let fanout_offset = 8
let hashes_offset = 8 + (256 * 4)

let bsearch idx hash =
  let n = string_get_int8 hash 0 in
  let a = if n = 0 then 0l else get_int32_be idx.mp (fanout_offset + 4 * (n - 1)) in
  let b = get_int32_be idx.mp (fanout_offset + 4 * n) in

  let abs_off = hashes_offset + Int32.to_int a * idx.uid_ln in
  let len = Int32.to_int (b <-> a) * idx.uid_ln in

  let rec go sub_off sub_len =
    let len = (sub_len / (2 * idx.uid_ln)) * idx.uid_ln in
    let cmp = compare_bigstring idx { off= sub_off + len; len } hash in

    if cmp == 0 then ( { off= sub_off + len; len } )
    else if sub_len <= idx.uid_ln then raise_notrace Not_found
    else if cmp > 0
    then (go[@tailcall]) sub_off len
    else (go[@tailcall]) (sub_off + len) (sub_len - len) in
  let { off; _ } = go abs_off len in
  Int32.to_int a + (off - abs_off) / idx.uid_ln

let isearch idx hash =
  let n = string_get_int8 hash 0 in
  let a = if n = 0 then 0l else get_int32_be idx.mp (fanout_offset + 4 * (n - 1)) in
  let b = get_int32_be idx.mp (fanout_offset + 4 * n) in

  let abs_off = hashes_offset + Int32.to_int a * idx.uid_ln in
  let len = Int32.to_int (b <-> a <-> 1l) * idx.uid_ln in

  let hashf = Int64.to_float (string_get_int64_be hash 0) in
  let uid_lnf = float_of_int idx.uid_ln in

  let rec go low high =
    if low + idx.uid_ln == high || low == high
    then ( let cmp = compare_bigstring idx { off= low; len= idx.uid_ln } hash in
           if cmp == 0 then { off= low; len= idx.uid_ln } else raise_notrace Not_found )
    else
      let lef = Int64.to_float (get_int64_be idx.mp low) in
      let hef = Int64.to_float (get_int64_be idx.mp high) in
      let lowf = float_of_int low in
      let highf = float_of_int high in

      let interpolation = floor ((highf -. lowf) *. (hashf -. lef) /. (hef -. lef)) in
      let off = lowf +. interpolation -. mod_float interpolation uid_lnf in
      let off = int_of_float off in
      let cmp = compare_bigstring idx { off; len= idx.uid_ln } hash in

      if cmp == 0 then { off; len= idx.uid_ln }
      else if cmp > 0
      then (go[@tailcall]) low off
      else (go[@tailcall]) (off + idx.uid_ln) high in
  let { off; _ } = go abs_off (abs_off + len) in
  Int32.to_int a + (off - abs_off) / idx.uid_ln

let find idx hash =
  let hash = idx.uid_rw hash in
  match isearch idx hash with
  | n ->
    let crcs_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) in
    let values_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) + (idx.n * 4) in

    let crc = get_int32_be idx.mp (crcs_offset + (n * 4)) in
    let off = get_int32_be idx.mp (values_offset + (n * 4)) in

    Some (Optint.of_int32 crc, Int32.to_int off)
  | exception Not_found -> None

let get_hash idx n =
  let res = Bytes.create idx.uid_ln in
  Bigstringaf.blit_to_bytes idx.mp ~src_off:(hashes_offset + (n * idx.uid_ln)) res ~dst_off:0 ~len:idx.uid_ln ;
  idx.uid_wr (Bytes.unsafe_to_string res)

let get_offset idx n =
  let values_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) + (idx.n * 4) in
  Int32.to_int (get_int32_be idx.mp (values_offset + (n * 4)))

let get_crc idx n =
  let crcs_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) in
  Optint.of_int32 (get_int32_be idx.mp (crcs_offset + (n * 4)))

let iter ~f idx =
  let rec go n =
    if n == idx.n then ()
    else
      let hash = get_hash idx n in
      let offset = get_offset idx n in
      let crc = get_crc idx n in
      f ~hash ~offset ~crc ; go (succ n) in
  go 0
