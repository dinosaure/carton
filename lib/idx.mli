type bigstring = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type 'uid idx
type optint = Optint.t

val make : bigstring -> uid_ln:int -> uid_rw:('uid -> string) -> uid_wr:(string -> 'uid) -> 'uid idx
val find : 'uid idx -> 'uid -> (optint * int) option
val iter : f:(hash:'uid -> offset:int -> crc:optint -> unit) -> 'uid idx -> unit

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

module N (Uid : UID) : sig
  type encoder

  type entry = { crc : optint; offset : int; uid : Uid.t }
  type dst = [ `Channel of out_channel | `Buffer of Buffer.t | `Manual ]

  val encoder : dst -> pack:Uid.t -> entry array -> encoder
  val encode : encoder -> [ `Await ] -> [ `Partial | `Ok ]
  val dst_rem : encoder -> int
  val dst : encoder -> Bigstringaf.t -> int -> int -> unit
end
