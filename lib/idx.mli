type bigstring = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type 'uid idx
type optint = Optint.t

val make : bigstring -> uid_ln:int -> uid_rw:('uid -> string) -> uid_wr:(string -> 'uid) -> 'uid idx
val find : 'uid idx -> 'uid -> (optint * int) option
val iter : f:(hash:'uid -> offset:int -> crc:optint -> unit) -> 'uid idx -> unit
