open Sigs

type 'uid entry
type 'uid delta = From of 'uid | Zero

val compare_entry : 'uid entry -> 'uid entry -> int
val make_entry : kind:kind -> length:int -> ?preferred:bool -> ?name:string -> ?delta:'uid delta -> 'uid -> 'uid entry

type 'uid q and 'uid p and 'uid patch

type ('uid, 's) load = 'uid -> (Dec.v, 's) io
type ('uid, 's) find = 'uid -> (int option, 's) io

type 'uid uid =
  { uid_ln : int
  ; uid_rw : 'uid -> string }

val target_to_source : 'uid q -> 'uid p
val target_uid : 'uid q -> 'uid

val entry_to_target : 's scheduler -> load:('uid, 's) load -> 'uid entry -> ('uid q, 's) io
val apply : 's scheduler -> load:('uid, 's) load -> uid_ln:int -> source:'uid p -> target:'uid q -> (unit, 's) io

module type VERBOSE = sig
  type 'a fiber

  val succ : unit -> unit fiber
  val print : unit -> unit fiber
end

module Delta
    (Scheduler : SCHEDULER)
    (IO : IO with type 'a t = 'a Scheduler.s)
    (Uid : UID)
    (Verbose : VERBOSE with type 'a fiber = 'a IO.t) : sig
  val s : Scheduler.t scheduler

  val delta :
    threads:(Uid.t, Scheduler.t) load list ->
    weight:int -> Uid.t entry array -> Uid.t q array IO.t
end

module N : sig
  type encoder

  type b =
    { i : Bigstringaf.t
    ; q : Dd.B.t
    ; w : Dd.window }

  val encoder : 's scheduler -> b:b -> load:('uid, 's) load -> 'uid q -> (encoder, 's) io
  val encode : o:Bigstringaf.t -> encoder -> [ `Flush of (encoder * int) | `End ]
  val dst : encoder -> Bigstringaf.t -> int -> int -> encoder
end

type b =
  { i : Bigstringaf.t
  ; q : Dd.B.t
  ; w : Dd.window
  ; o : Bigstringaf.t }

val header_of_pack : length:int -> Bigstringaf.t -> int -> int -> unit
val encode_target : 's scheduler -> b:b -> find:('uid, 's) find -> load:('uid, 's) load -> uid:'uid uid -> 'uid q -> cursor:int -> (int * N.encoder, 's) io
