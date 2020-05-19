open Carton

module Make
    (Scheduler : SCHEDULER)
    (IO : IO with type 'a t = 'a Scheduler.s)
    (Uid : UID) : sig
  type ('path, 'fd, 'error) fs =
    { create : 'path -> ('fd, 'error) result IO.t
    ; append : 'fd -> string -> unit IO.t
    ; map : 'fd -> pos:int64 -> int -> Bigstringaf.t IO.t
    ; close : 'fd -> unit IO.t }

  val verify
    :  ?threads:int
    -> digest:Uid.t Carton.Dec.digest
    -> 'path
    -> ('path, 'fd, [> `Msg of string ] as 'error) fs
    -> (unit -> (string * int * int) option IO.t)
    -> (int * Uid.t list * int64, 'error) result IO.t
  (** [verify ~digest filename fs stream] does the first pass to analyze
     a PACK file. While it analyzes the PACK file, it saves it into
     [filename] with the [fs]'s [append] {i syscall}. Then, it returns
     how many objects has the stream, the list of required external objects
     and the size of the stream.

      If the list is empty, the given stream (saved into [filename]) is a
     {i canonic} PACK file. It does not require any external objects to extract
     any of its objects.

      Otherwise, you probably should call {!canonicalize} to regenerate the
     PACK file. *)

  type light_load = Uid.t -> (Carton.kind * int) IO.t
  type heavy_load = Uid.t -> Carton.Dec.v IO.t
  type transmit = brk:int64 -> (bytes * int * int) IO.t

  val canonicalize
    :  light_load:light_load
    -> heavy_load:heavy_load
    -> transmit:transmit
    -> 'path
    -> ('path, 'fd, [> `Msg of string ] as 'error) fs
    -> int
    -> Uid.t list
    -> int64
    -> (int64, 'error) result IO.t
  (** [canonicalize ~light_load ~heavy_load ~transmit filename fs n requireds weight]
     generates a new PACK file with required objects [requireds]. It puts on the front
     these objects available with [light_load] and [heavy_load].

      Then, it transmits all others objects of the old PACK file to the new one with
     [transmit]. It must know how many objects has the old PACK file and size of it.

      It returns the size of the new PACK file generated located to [filename]. *)
end
