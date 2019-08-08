let input_bigstring ic buf off len =
  let tmp = Bytes.create len in
  let res = input ic tmp 0 len in
  Bigstringaf.blit_from_bytes tmp ~src_off:0 buf ~dst_off:off ~len:res ; res

module M : sig
  type decoder

  type src = [ `Channel of in_channel | `String of string | `Manual ]
  type decode = [ `Await of decoder | `Header of (int * int * decoder) | `End of decoder | `Malformed of string ]

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
    ; h : H.M.decoder }

  let refill k d = match d.src with
    | `String _ ->
      let z = Zz.M.src d.z Dd.bigstring_empty 0 0 in
      k { d with z }
    | `Channel ic ->
      let res = input_bigstring ic d.i 0 (Dd.bigstring_length d.i) in
      let z = Zz.M.src d.z d.i 0 res in
      k { d with z }
    | `Manual -> `Await d

  let rec decode d =
    match H.M.decode d.h with
    | `Header (src_len, dst_len) ->
      `Header (src_len, dst_len, { d with src_len; dst_len; })
    | `End -> `End d
    | `Malformed err -> `Malformed err
    | `Await ->
      let rec inflate d = match Zz.M.decode d.z with
        | `Await z ->
          refill inflate { d with z }
        | `End z ->
          let dst_len = Dd.bigstring_length d.o - Zz.M.dst_rem z in
          H.M.src d.h d.o 0 dst_len ; decode { d with z }
        | `Flush z ->
          let dst_len = Dd.bigstring_length d.o - Zz.M.dst_rem z in
          H.M.src d.h d.o 0 dst_len ; decode { d with z }
        | `Malformed err -> `Malformed err in
      inflate { d with z= Zz.M.flush d.z }

  let src d s j l =
    let z = Zz.M.src d.z s j l in
    { d with z }

  let dst d s j l =
    H.M.dst d.h s j l ; d

  let source d src =
    H.M.source d.h src ; d

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
    ; h= decoder_h }

  type decode = [ `Await of decoder
                | `Header of (int * int * decoder)
                | `End of decoder
                | `Malformed of string ]
end
