module type FUNCTOR = sig type 'a t end
type ('a, 's) io

type 's scheduler =
  { bind : 'a 'b. ('a, 's) io -> ('a -> ('b, 's) io) -> ('b, 's) io
  ; return : 'a. 'a -> ('a, 's) io }

module type SCHEDULER = sig
  type 'a s
  type t

  external inj : 'a s -> ('a, t) io = "%identity"
  external prj : ('a, t) io -> 'a s = "%identity"
end

module Make (T : FUNCTOR) : SCHEDULER with type 'a s = 'a T.t

type bigstring = Bigstringaf.t

module W : sig
  type 'fd t
  and slice =
    { offset : int
    ; length : int
    ; payload : bigstring }
  and ('fd, 's) map = 'fd -> pos:int -> int -> (bigstring, 's) io
end

module Uid : sig
  type t = Digestif.SHA1.t
  type ctx = Digestif.SHA1.ctx

  val empty : ctx
  val get : ctx -> t
  val feed : ctx -> ?off:int -> ?len:int -> bigstring -> ctx
  val equal : t -> t -> bool
  val length : int
  val of_raw_string : string -> t
  val pp : t Fmt.t
end

type weight = private int

val null : weight
val weight_of_int_exn : int -> weight

module Fpass : sig
  type kind =
    | Base of [ `A | `B | `C | `D ]
    | Ofs of { sub : int; source : weight; target : weight; }
    | Ref of { ptr : Uid.t; source : int; target : int; }

  type entry =
    { offset : int
    ; kind : kind
    ; size : weight
    ; consumed : int }

  type decoder

  type src =
    [ `Channel of in_channel | `String of string | `Manual ]
  type decode =
    [ `Await of decoder
    | `Peek of decoder
    | `Entry of (entry * decoder)
    | `End
    | `Malformed of string ]

  val decoder : o:Bigstringaf.t -> allocate:(int -> Dd.window) -> src -> decoder
  val decode : decoder -> decode

  val number : decoder -> int
  val version : decoder -> int
  val count : decoder -> int
end

type 'fd t

type raw

val make_raw : weight:weight -> raw

type v
type kind = [ `A | `B | `C | `D ]

val kind : v -> kind
val raw : v -> bigstring
val len : v -> int

val make : 'fd -> z:Zz.bigstring -> allocate:(int -> Zz.window) -> (Uid.t -> int) -> 'fd t

(** {3 Weight of object.} *)

val weight_of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> weight:weight -> cursor:int -> (weight, 's) io
val weight_of_uid : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> weight:weight -> Uid.t -> (weight, 's) io

val of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> raw -> cursor:int -> (v, 's) io
val of_uid : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> raw -> Uid.t -> (v, 's) io

(** {3 Path of object.} *)

type path

val pp_path : path Fmt.t

val path_of_offset : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> cursor:int -> (path, 's) io
val of_offset_with_path : 's scheduler -> map:('fd, 's) W.map -> 'fd t -> path:path -> raw -> cursor:int -> (v, 's) io

(** {3 Uid of object.} *)

type digest = kind:kind -> ?off:int -> ?len:int -> bigstring -> Uid.t

val uid_of_offset : 's scheduler -> map:('fd, 's) W.map -> digest:digest -> 'fd t -> raw -> cursor:int -> (kind * Uid.t, 's) io
val uid_of_offset_with_source : 's scheduler -> map:('fd, 's) W.map -> digest:digest -> 'fd t -> kind:kind -> raw -> cursor:int -> (Uid.t, 's) io

type children = cursor:int -> uid:Uid.t -> int list
type where = cursor:int -> int

type oracle =
  { digest : digest
  ; children : children
  ; where : where
  ; weight : cursor:int -> weight }

module type MUTEX = sig
  type 'a fiber
  type t

  val create : unit -> t
  val lock : t -> unit fiber
  val unlock : t -> unit
end

module type FUTURE = sig
  type 'a fiber
  type 'a t

  val wait : 'a t -> 'a fiber
  val peek : 'a t -> 'a option
end

module type IO = sig
  type 'a t

  module Future : FUTURE with type 'a fiber = 'a t
  module Mutex : MUTEX with type 'a fiber = 'a t

  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val return : 'a -> 'a t

  val nfork_map : 'a list -> f:('a -> 'b t) -> 'b Future.t list t
  val all_unit : unit t list -> unit t
end

module Verify (IO : IO) : sig
  module Scheduler : module type of Make(IO)

  val s : Scheduler.t scheduler

  type status

  val uid_of_status : status -> Uid.t
  val kind_of_status : status -> kind
  val depth_of_status : status -> int
  val source_of_status : status -> Uid.t option

  val unresolved_base : cursor:int -> status
  val unresolved_node : status

  val verify : map:('fd, Scheduler.t) W.map -> oracle:oracle -> 'fd t -> matrix:status array -> unit IO.t
end
