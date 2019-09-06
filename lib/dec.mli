open Sigs

module W : sig
  type 'fd t
  and slice =
    { offset : int
    ; length : int
    ; payload : Bigstringaf.t }
  and ('fd, 's) map = 'fd -> pos:int -> int -> (Bigstringaf.t, 's) io

  val make : 'fd -> 'fd t
end

type weight = private int

val null : weight
val weight_of_int_exn : int -> weight

type ('fd, 's) read = 'fd -> bytes -> off:int -> len:int -> (int, 's) io

module Idx = Idx

module Fp (Uid : UID) : sig
  type optint = Optint.t

  type kind =
    | Base of [ `A | `B | `C | `D ]
    | Ofs of { sub : int; source : weight; target : weight; }
    | Ref of { ptr : Uid.t; source : int; target : int; }

  type entry =
    { offset : int
    ; kind : kind
    ; size : weight
    ; consumed : int
    ; crc : optint }

  val check_header : 's scheduler -> ('fd, 's) read -> 'fd -> (int, 's) io

  type decoder

  type src =
    [ `Channel of in_channel | `String of string | `Manual ]
  type decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Entry of (entry * decoder)
    | `End of Uid.t
    | `Malformed of string ]

  val decoder : o:Bigstringaf.t -> allocate:(int -> Dd.window) -> src -> decoder
  val decode : decoder -> decode

  val number : decoder -> int
  val version : decoder -> int
  val count : decoder -> int
end

type ('fd, 'uid) t
(** Type of state used to access to any objects into a [Carton] file. *)

val with_z : Bigstringaf.t -> ('fd, 'uid) t -> ('fd, 'uid) t
val with_w : 'fd W.t -> ('fd, 'uid) t -> ('fd, 'uid) t
val with_allocate : allocate:(int -> Dd.window) -> ('fd, 'uid) t -> ('fd, 'uid) t

type raw
(** Type of a [Carton] object as is into a [Carton] file. *)

val make_raw : weight:weight -> raw
val weight_of_raw : raw -> weight

type v

val v : kind:kind -> ?depth:int -> Bigstringaf.t -> v
val kind : v -> kind
val raw : v -> Bigstringaf.t
val len : v -> int
val depth : v -> int

val make : 'fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> uid_ln:int -> uid_rw:(string -> 'uid) -> ('uid -> int) -> ('fd, 'uid) t
(** [make fd ~z ~allocate ~uid_ln ~uid_rw where] returns a state associated to
   [fd] which is the user-defined representation of a [Carton] file. Some
   informations are needed:

   {ul
    {- [z] is an underlying buffer used to {i inflate} an object.}
    {- [allocate] is an {i allocator} of underlying {i window} used to {i
   inflate} an object.}
    {- [uid_ln] is the length of {i raw} representation of user-defined {i
   uid}.}
    {- [uid_rw] is the {i cast-function} from a string to user-defined {i uid}.}
    {- [where] is the function to associate an {i uid} to an {i offset} into the
   associated [Carton] file.} }

   Each argument depends on what the user wants. For example, if [t] is used by
   {!Verify.verify}, [allocate] {b must} be thread-safe according to {!IO}.
   [where] is not used by {!Verify.verify}. [uid_ln] and [uid_rw] depends on the
   [Carton] file associated by [fd]. Each functions available below describes
   precisely what they do on [t]. *)

(** {3 Weight of object.} *)

val weight_of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> weight:weight -> cursor:int -> (weight, 's) io
val weight_of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> weight:weight -> 'uid -> (weight, 's) io

(** {3 Value of object.} *)

val of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> raw -> cursor:int -> (v, 's) io
val of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> raw -> 'uid -> (v, 's) io

(** {3 Path of object.} *)

type path

val path_to_list : path -> int list

val path_of_offset : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> cursor:int -> (path, 's) io
val path_of_uid : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> 'uid -> (path, 's) io
val of_offset_with_path : 's scheduler -> map:('fd, 's) W.map -> ('fd, 'uid) t -> path:path -> raw -> cursor:int -> (v, 's) io

(** {3 Uid of object.} *)

type 'uid digest = kind:kind -> ?off:int -> ?len:int -> Bigstringaf.t -> 'uid

val uid_of_offset : 's scheduler -> map:('fd, 's) W.map -> digest:'uid digest -> ('fd, 'uid) t -> raw -> cursor:int -> (kind * 'uid, 's) io
val uid_of_offset_with_source : 's scheduler -> map:('fd, 's) W.map -> digest:'uid digest -> ('fd, 'uid) t -> kind:kind -> raw -> depth:int -> cursor:int -> ('uid, 's) io

type 'uid children = cursor:int -> uid:'uid -> int list
type where = cursor:int -> int

type 'uid oracle =
  { digest : 'uid digest
  ; children : 'uid children
  ; where : where
  ; weight : cursor:int -> weight }

module Verify (Uid : UID) (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) : sig
  val s : Scheduler.t scheduler

  type status

  val uid_of_status : status -> Uid.t
  val kind_of_status : status -> kind
  val depth_of_status : status -> int
  val source_of_status : status -> Uid.t option

  val unresolved_base : cursor:int -> status
  val unresolved_node : status

  val verify : threads:int -> map:('fd, Scheduler.t) W.map -> oracle:Uid.t oracle -> ('fd, Uid.t) t -> matrix:status array -> unit IO.t
end

module Ip (Scheduler : SCHEDULER) (IO : IO with type 'a t = 'a Scheduler.s) (Uid : UID): sig
  val iter : threads:'a list -> f:('a -> uid:Uid.t -> offset:int -> crc:Idx.optint -> unit IO.t) -> Uid.t Idx.idx -> unit IO.t
end
