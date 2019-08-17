module Uid : sig
  type t
end

type kind =
  | Base of [ `A | `B | `C | `D ]
  | Ofs of { sub : int; source : int; target : int; }
  | Ref of { ptr : Uid.t; source : int; target : int; }

type entry = { offset : int; kind : kind; size : int; }

type decoder

type src = [ `Channel of in_channel | `String of string | `Manual ]
type decode = [ `Await of decoder | `Peek of decoder | `Entry of (entry * decoder) | `End | `Malformed of string ]

val decoder : o:Bigstringaf.t -> allocate:(int -> Dd.window) -> src -> decoder
val decode : decoder -> decode

val number : decoder -> int
val version : decoder -> int
val count : decoder -> int
