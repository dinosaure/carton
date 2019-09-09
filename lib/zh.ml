let input_bigstring ic buf off len =
  let tmp = Bytes.create len in
  let res = input ic tmp 0 len in
  Bigstringaf.blit_from_bytes tmp ~src_off:0 buf ~dst_off:off ~len:res ; res

let output_bigstring oc buf off len =
  let res = Bigstringaf.substring buf ~off ~len in
  output_string oc res

module N : sig
  type encoder
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]
  type ret = [ `Flush of encoder | `End ]

  val dst_rem : encoder -> int
  val dst : encoder -> Zz.bigstring -> int -> int -> encoder

  val encode : encoder -> ret
  val encoder : i:Zz.bigstring -> q:Dd.B.t -> w:Zz.window -> source:int -> H.bigstring -> dst -> Duff.hunk list -> encoder
end = struct
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  type encoder =
    { dst : dst
    ; src : Bigstringaf.t
    ; o : H.bigstring
    ; o_pos : int
    ; o_max : int
    ; h : H.N.encoder
    ; z : Zz.N.encoder
    ; t : Zz.bigstring
    ; d : [ `Copy of (int * int) | `Insert of string | `End | `Await ] list }

  type ret = [ `Flush of encoder | `End ]

  let flush k e = match e.dst with
    | `Manual -> `Flush e
    | `Channel oc ->
      output_bigstring oc e.o 0 e.o_pos ; k { e with o_pos= 0 }
    | `Buffer b ->
      for i = 0 to e.o_pos - 1
      do Buffer.add_char b (Bigstringaf.get e.o i) done ;
      k { e with o_pos= 0 }

  let rec encode_z e = match Zz.N.encode e.z with
    | `End z ->
      let len = Bigstringaf.length e.o - Zz.N.dst_rem z in
      let z = Zz.N.dst z Dd.bigstring_empty 0 0 in

      if len > 0
      then flush encode_z { e with z; o_pos= len }
      else `End
    | `Flush z ->
      let len = Bigstringaf.length e.o - Zz.N.dst_rem z in
      flush encode_z { e with z; o_pos= len }
    | `Await z ->
      match e.d with
      | [] ->
        let z = Zz.N.src z Dd.bigstring_empty 0 0 in
        encode_z { e with z }
      | d ->
        H.N.dst e.h e.t 0 (Dd.bigstring_length e.t) ; encode_h e d

  and encode_h e d =
    let v, d = match d with
      | v :: d -> v, d
      | [] -> `End, [] in
    match H.N.encode e.h v, d with
    | `Ok, [] ->
      let len = Bigstringaf.length e.t - H.N.dst_rem e.h in
      let z = Zz.N.src e.z e.t 0 len in

      encode_z { e with d; z }
    | `Ok, d ->
      encode_h { e with d } d
    | `Partial, d ->
      let len = Bigstringaf.length e.t - H.N.dst_rem e.h in
      let z = Zz.N.src e.z e.t 0 len in

      encode_z { e with d= `Await :: d; z }

  let encode e = encode_z e

  let encoder ~i ~q ~w ~source src dst hunks =
    let o, o_pos, o_max = match dst with
      | `Manual -> Dd.bigstring_empty, 1, 0
      | `Buffer _
      | `Channel _ -> Dd.bigstring_create H.io_buffer_size, 0, H.io_buffer_size - 1 in
    let z = Zz.N.encoder `Manual `Manual ~q ~w ~level:0 in
    let z = Zz.N.dst z Dd.bigstring_empty 0 0 in
    { dst
    ; src
    ; o; o_pos; o_max
    ; t= i
    ; d= List.map (function Duff.Copy (off, len) -> `Copy (off, len) | Duff.Insert (off, len) -> `Insert (Bigstringaf.substring src ~off ~len)) hunks
    ; z
    ; h= H.N.encoder `Manual ~dst_len:(Bigstringaf.length src) ~src_len:source }

  let dst_rem e = e.o_max - e.o_pos + 1

  let dst e s j l =
    let z = Zz.N.dst e.z s j l in
    { e with z; o= s; o_pos= j; o_max= j + l - 1 }
end

module M : sig
  type decoder

  type src = [ `Channel of in_channel | `String of string | `Manual ]
  type decode = [ `Await of decoder | `Header of (int * int * decoder) | `End of decoder | `Malformed of string ]

  val src_len : decoder -> int
  val dst_len : decoder -> int
  val src_rem : decoder -> int
  val dst_rem : decoder -> int

  val src : decoder -> Zz.bigstring -> int -> int -> decoder
  val dst : decoder -> H.bigstring -> int -> int -> decoder
  val source : decoder -> H.bigstring -> decoder
  val decode : decoder -> decode
  val decoder : ?source:H.bigstring -> o:Zz.bigstring -> allocate:(int -> Zz.window) -> src -> decoder
end = struct
  type src = [ `Channel of in_channel | `String of string | `Manual ]

  type decoder =
    { src : src
    ; dst : H.bigstring
    ; dst_len : int
    ; src_len : int
    ; i : Zz.bigstring
    ; i_pos : int
    ; i_len : int
    ; o : Zz.bigstring
    ; z : Zz.M.decoder
    ; h : H.M.decoder
    ; k : decoder -> decode }
  and decode = [ `Await of decoder
               | `Header of (int * int * decoder)
               | `End of decoder
               | `Malformed of string ]

  let refill k d = match d.src with
    | `String _ ->
      let z = Zz.M.src d.z Dd.bigstring_empty 0 0 in
      k { d with z }
    | `Channel ic ->
      let res = input_bigstring ic d.i 0 (Dd.bigstring_length d.i) in
      let z = Zz.M.src d.z d.i 0 res in
      k { d with z }
    | `Manual -> `Await { d with k }

  let rec decode d =
    match H.M.decode d.h with
    | `Header (src_len, dst_len) ->
      `Header (src_len, dst_len, { d with src_len; dst_len; k= decode; })
    | `End -> `End { d with k= decode }
    | `Malformed err -> `Malformed err
    | `Await ->
      inflate { d with z= Zz.M.flush d.z }
  and inflate d =
    match Zz.M.decode d.z with
    | `Await z ->
      let dst_len = Dd.bigstring_length d.o - Zz.M.dst_rem z in
      H.M.src d.h d.o 0 dst_len ; refill inflate { d with z }
    | `End z ->
      let dst_len = Dd.bigstring_length d.o - Zz.M.dst_rem z in
      H.M.src d.h d.o 0 dst_len ; decode { d with z }
    | `Flush z ->
      let dst_len = Dd.bigstring_length d.o - Zz.M.dst_rem z in
      H.M.src d.h d.o 0 dst_len ; decode { d with z }
    | `Malformed err -> `Malformed err

  let src d s j l =
    let z = Zz.M.src d.z s j l in
    { d with z }

  let dst d s j l =
    H.M.dst d.h s j l ; d

  let source d src =
    H.M.source d.h src ; d

  let dst_len d =
    let dst_len = H.M.dst_len d.h in
    assert (d.dst_len = dst_len) ; dst_len

  let src_len d =
    let src_len = H.M.src_len d.h in
    assert (d.src_len = src_len) ; src_len

  let dst_rem d = H.M.dst_rem d.h
  let src_rem d = Zz.M.src_rem d.z

  let decoder ?source ~o ~allocate src =
    let decoder_z = Zz.M.decoder `Manual ~o ~allocate in
    let decoder_h = H.M.decoder `Manual ?source in

    let i, i_pos, i_len = match src with
      | `Manual -> Dd.bigstring_empty, 1, 0
      | `String x ->
        Bigstringaf.of_string x ~off:0 ~len:(String.length x),
        0, String.length x - 1
      | `Channel _ -> Bigstringaf.create Dd.io_buffer_size, 1, 0 in

    { src
    ; dst= Dd.bigstring_empty
    ; dst_len= 0
    ; src_len= 0
    ; i
    ; i_pos
    ; i_len
    ; o
    ; z= decoder_z
    ; h= decoder_h
    ; k= decode }

  let decode d = d.k d
end
