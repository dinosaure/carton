module N : sig
  type encoder
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]
  type ret = [ `Flush of encoder | `End ]

  val dst_rem : encoder -> int
  val dst : encoder -> Zz.bigstring -> int -> int -> encoder

  val encode : encoder -> ret
  val encoder : i:Zz.bigstring -> q:Dd.B.t -> w:Zz.window -> source:int -> H.bigstring -> dst -> Duff.hunk list -> encoder
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
end
