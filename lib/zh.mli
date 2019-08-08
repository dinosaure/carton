module M : sig
  type decoder

  type src = [ `Channel of in_channel | `String of string | `Manual ]
  type decode = [ `Await of decoder | `Header of (int * int * decoder) | `End of decoder | `Malformed of string ]

  val src : decoder -> Zz.bigstring -> int -> int -> decoder
  val dst : decoder -> H.bigstring -> int -> int -> decoder
  val source : decoder -> H.bigstring -> decoder
  val decode : decoder -> decode
  val decoder : ?source:H.bigstring -> o:Zz.bigstring -> allocate:(int -> Zz.window) -> src -> decoder
end
