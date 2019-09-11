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

    if i <> 0xff744f63l then Fmt.invalid_arg "Invalid IDX file (header: %lx <> %lx)" i 0xff744f63l ;
    if v <> 0x2l then Fmt.invalid_arg "Invalid version of IDX file" ;
    { mp; n= Int32.to_int n; uid_ln; uid_rw; uid_wr; }

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

    let res0 = (!c1 land 0xff) - (!c2 land 0xff) in
    let res1 = (!c1 asr 8) - (!c2 asr 8) in
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
  Fmt.epr "uid: %a.\n%!" Digestif.SHA1.pp (Obj.magic hash) ;
  let { off; _ } = go abs_off len in
  Fmt.epr "off: %d.\n%!" off ;
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
    if low == high
    then ( let cmp = compare_bigstring idx { off= low; len= idx.uid_ln } hash in
           if cmp == 0
           then { off= low; len= idx.uid_ln }
           else raise_notrace Not_found )
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
      then (go[@tailcall]) low (off - idx.uid_ln)
      else (go[@tailcall]) (off + idx.uid_ln) high in
  if len < 0 then raise_notrace Not_found ;

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

    Some (Optint.of_int32 crc, Int64.of_int32 off)
  | exception Not_found ->
    Fmt.epr "%a not found.\n%!" Digestif.SHA1.pp (Obj.magic hash) ;
    None

let exists idx uid =
  let uid = idx.uid_rw uid in
  match isearch idx uid with
  | _ -> true
  | exception Not_found -> false

let get_uid idx n =
  let res = Bytes.create idx.uid_ln in
  Bigstringaf.blit_to_bytes idx.mp ~src_off:(hashes_offset + (n * idx.uid_ln)) res ~dst_off:0 ~len:idx.uid_ln ;
  idx.uid_wr (Bytes.unsafe_to_string res)

let get_offset idx n =
  let values_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) + (idx.n * 4) in
  Int64.of_int32 (get_int32_be idx.mp (values_offset + (n * 4)))

let get_crc idx n =
  let crcs_offset = 8 + (256 * 4) + (idx.n * idx.uid_ln) in
  Optint.of_int32 (get_int32_be idx.mp (crcs_offset + (n * 4)))

let max { n; _ } = n

let iter ~f idx =
  let rec go n =
    if n == idx.n then ()
    else
      let uid = get_uid idx n in
      let offset = get_offset idx n in
      let crc = get_crc idx n in
      f ~uid ~offset ~crc ; go (succ n) in
  go 0

module type UID = sig
  type t
  type ctx

  val empty : ctx
  val feed : ctx -> ?off:int -> ?len:int -> bigstring -> ctx
  val get : ctx -> t

  val compare : t -> t -> int
  val length : int
  val to_raw_string : t -> string
end

module N (Uid : UID): sig
  type encoder

  type entry = { crc : optint; offset : int64; uid : Uid.t }
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  val encoder : dst -> pack:Uid.t -> entry array -> encoder
  val encode : encoder -> [ `Await ] -> [ `Partial | `Ok ]
  val dst_rem : encoder -> int
  val dst : encoder -> Bigstringaf.t -> int -> int -> unit
end = struct
  type entry = { crc : optint; offset : int64; uid : Uid.t }
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  type encoder =
    { dst : dst
    ; mutable o : bigstring
    ; mutable o_pos : int
    ; mutable o_max : int
    ; t : bigstring
    ; mutable t_pos : int
    ; mutable t_max : int
    ; mutable n : int
    ; fanout : int array
    ; index : entry array
    ; pack : Uid.t
    ; mutable ctx : Uid.ctx
    ; mutable k : encoder -> [ `Await ] -> [ `Partial | `Ok ] }

  let dst e s j l =
    if (j < 0 || l < 0 || j + l > Bigstringaf.length s)
    then Fmt.invalid_arg "Out of bounds (off: %d, len: %d)" j l ;
    e.o <- s ; e.o_pos <- j ; e.o_max <- j + l - 1

  let partial k e = function `Await -> k e

  let flush_with_ctx k e = match e.dst with
    | `Manual ->
      let ctx = Uid.feed e.ctx ~off:0 ~len:e.o_pos e.o in
      e.ctx <- ctx ; e.k <- partial k ; `Partial
    | `Channel oc ->
      let raw = Bigstringaf.substring e.o ~off:0 ~len:e.o_pos in
      let ctx = Uid.feed e.ctx ~off:0 ~len:e.o_pos e.o in
      output_string oc raw ; e.o_pos <- 0 ; e.ctx <- ctx ; k e
    | `Buffer b ->
      let raw = Bigstringaf.substring e.o ~off:0 ~len:e.o_pos in
      let ctx = Uid.feed e.ctx ~off:0 ~len:e.o_pos e.o in
      Buffer.add_string b raw ; e.o_pos <- 0 ; e.ctx <- ctx ; k e

  let flush_without_ctx k e = match e.dst with
    | `Manual -> e.k <- partial k ; `Partial
    | `Channel oc ->
      let raw = Bigstringaf.substring e.o ~off:0 ~len:e.o_pos in
      output_string oc raw ; e.o_pos <- 0 ; k e
    | `Buffer b ->
      let raw = Bigstringaf.substring e.o ~off:0 ~len:e.o_pos in
      Buffer.add_string b raw ; e.o_pos <- 0 ; k e

  let o_rem e = e.o_max - e.o_pos + 1

  let t_range e m = e.t_pos <- 0 ; e.t_max <- m

  let rec t_flush ?(with_ctx= true) k e =
    let blit e l =
      Bigstringaf.blit e.t ~src_off:e.t_pos e.o ~dst_off:e.o_pos ~len:l ;
      e.o_pos <- e.o_pos + l ; e.t_pos <- e.t_pos + l in
    let rem = o_rem e in
    let len = e.t_max - e.t_pos + 1 in
    let flush = if with_ctx then flush_with_ctx else flush_without_ctx in
    if rem < len then ( blit e rem ; flush (t_flush k) e ) else ( blit e len ; k e )

  let ok e =
    e.k <- (fun _ `Await -> `Ok) ; `Ok

  let encode_trail e `Await =
    let k2 e = flush_without_ctx ok e in
    let k1 e =
      let rem = o_rem e in
      let s, j, k =
        if rem < Uid.length
        then ( t_range e (Uid.length - 1) ; e.t, 0, t_flush ~with_ctx:false k2 )
        else let j = e.o_pos in ( e.o_pos <- e.o_pos + Uid.length ; e.o, j, k2 ) in
      let uid = Uid.get e.ctx in
      let uid = Uid.to_raw_string uid in
      Bigstringaf.blit_from_string uid ~src_off:0 s ~dst_off:j ~len:Uid.length ;
      k e in
    let k0 e = flush_with_ctx k1 e in
    let rem = o_rem e in
    let s, j, k =
      if rem < Uid.length
      then ( t_range e (Uid.length - 1) ; e.t, 0, t_flush k0 )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + Uid.length ; e.o, j, k0 ) in
    let uid = Uid.to_raw_string e.pack in
    Bigstringaf.blit_from_string uid ~src_off:0 s ~dst_off:j ~len:Uid.length ;
    k e

  let rec encode_offset e `Await =
    let k e =
      if e.n + 1 == Array.length e.index
      then ( e.n <- 0 ; encode_trail e `Await )
      else ( e.n <- succ e.n
           ; encode_offset e `Await ) in
    let rem = o_rem e in

    let s, j, k =
      if rem < 4
      then ( t_range e 3 ; e.t, 0, t_flush k )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + 4 ; e.o, j, k ) in
    let { offset; _ } = e.index.(e.n) in
    Bigstringaf.set_int32_be s j (Int64.to_int32 offset) ;
    k e

  let rec encode_crc e `Await =
    let k e =
      if e.n + 1 == Array.length e.index
      then ( e.n <- 0 ; encode_offset e `Await )
      else ( e.n <- succ e.n
           ; encode_crc e `Await ) in
    let rem = o_rem e in

    let s, j, k =
      if rem < 4
      then ( t_range e 3 ; e.t, 0, t_flush k )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + 4 ; e.o, j, k ) in
    let { crc; _ } = e.index.(e.n) in
    Bigstringaf.set_int32_be s j (Optint.to_int32 crc) ;
    k e

  let rec encode_hash e `Await =
    let k e =
      if e.n + 1 == Array.length e.index
      then ( e.n <- 0 ; encode_crc e `Await )
      else ( e.n <- succ e.n
           ; encode_hash e `Await ) in
    let rem = o_rem e in

    let s, j, k =
      if rem < Uid.length
      then ( t_range e (Uid.length - 1) ; e.t, 0, t_flush k )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + Uid.length ; e.o, j, k ) in
    let { uid; _ } = e.index.(e.n) in

    Bigstringaf.blit_from_string (Uid.to_raw_string uid) ~src_off:0 s ~dst_off:j ~len:Uid.length ;
    k e

  let rec encode_fanout e `Await =
    let k e =
      if e.n + 1 == 256
      then ( e.n <- 0 ; encode_hash e `Await )
      else ( e.n <- succ e.n
           ; encode_fanout e `Await ) in
    let rem = o_rem e in

    let s, j, k =
      if rem < 4
      then ( t_range e 3 ; e.t, 0, t_flush k )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + 4 ; e.o, j, k ) in
    let x = let acc = ref 0 in for i = 0 to e.n do acc := !acc + e.fanout.(i) done ; !acc in
    Bigstringaf.set_int32_be s j (Int32.of_int x) ;
    k e

  let encode_header e `Await =
    let k e = e.n <- 0 ; encode_fanout e `Await in
    let rem = o_rem e in
    let s, j, k =
      if rem < 8
      then ( t_range e 8 ; e.t, 0, t_flush k )
      else let j = e.o_pos in ( e.o_pos <- e.o_pos + 8 ; e.o, j, k ) in
    Bigstringaf.set_int32_be s j 0xff744f63l ;
    Bigstringaf.set_int32_be s (j + 4) 0x2l ;
    k e

  let io_buffer_size = 65536

  let encoder dst ~pack index =
    Array.sort (fun { uid= a; _ } { uid= b; _ } -> Uid.compare a b) index ;
    let fanout = Array.make 256 0 in
    Array.iter (fun { uid; _ } -> let n = Char.code (Uid.to_raw_string uid).[0] in fanout.(n) <- fanout.(n) + 1) index ;
    let o, o_pos, o_max = match dst with
      | `Manual -> Bigstringaf.empty, 1, 0
      | `Buffer _
      | `Channel _ -> Bigstringaf.create io_buffer_size, 0, io_buffer_size - 1 in
    { dst
    ; o; o_pos; o_max
    ; t= Bigstringaf.create Uid.length
    ; t_pos= 1
    ; t_max= 0
    ; n= 0
    ; fanout; index; pack
    ; ctx= Uid.empty
    ; k= encode_header }

  let dst_rem = o_rem
  let encode e = e.k e
end
