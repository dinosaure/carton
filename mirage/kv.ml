module type MEMORY = sig
  type t
  type 'a fiber

  val map : t -> pos:int64 -> int -> Bigstringaf.t fiber
  val disconnect : t -> unit fiber
end

module type OBJECT = sig
  type uid
  type t
  type error = Mirage_kv.error
  type 'a fiber

  val pp_error : error Fmt.t
  val digest : Mirage_kv.Key.t -> (uid, error) result fiber
  val v : kind:[ `A | `B | `C | `D ] -> Bigstringaf.t -> off:int -> len:int -> (t, error) result
  val is : [ `A | `B | `C | `D ] -> [ `Dictionary | `Value ]
  val iter : t -> ((string * [ `Value | `Dictionary ]) list, error) result
end

type ('fd, 'uid) t =
  { pack : ('fd, 'uid) Carton.Dec.t
  ; idx : 'uid Carton.Dec.Idx.idx
  ; fd_idx : 'fd
  ; mp_idx : Bigstringaf.t }

module type RO = sig 
  type t
  type +'a io
  type error
  type value
  type key = Mirage_kv.Key.t

  val disconnect : t -> unit io
  val pp_error : error Fmt.t
  val exists : t -> key -> ([ `Dictionary | `Value ] option, error) result io
  val get : t -> key -> (value, error) result io
  val list : t -> key -> ((string * [ `Dictionary | `Value ]) list, error) result io
  val last_modified : t -> key -> (int * int64, error) result io
  val digest : t -> key -> (string, error) result io
end

module Make
    (Uid : Carton.UID)
    (Scheduler : Carton.SCHEDULER)
    (IO : Carton.IO with type 'a t = 'a Scheduler.s)
    (Object : OBJECT with type uid = Uid.t and type 'a fiber = 'a IO.t)
    (Memory : MEMORY with type 'a fiber = 'a IO.t)
  : RO with type value = Object.t
        and type +'a io = 'a IO.t
        and type error = private [> Mirage_kv.error ]
= struct
  let scheduler =
    let open Scheduler in
    { Carton.bind= (fun x f -> inj (IO.bind (prj x) (fun x -> prj (f x))))
    ; Carton.return= (fun x -> inj (IO.return x)) }

  type 'a io = 'a IO.t

  type nonrec t = (Memory.t, Uid.t) t
  type key = Mirage_kv.Key.t
  type value = Object.t
  type error = Mirage_kv.error

  let pp_error = Mirage_kv.pp_error
  let ( >>= ) = IO.bind

  let disconnect t =
    let ( >>= ) = IO.bind in
    Memory.disconnect t.fd_idx >>= fun () ->
    Memory.disconnect (Carton.Dec.fd t.pack)

  let exists { idx; pack; _ } key =
    Object.digest key >>= function
    | Error (#Mirage_kv.error as err) -> IO.return (Error err)
    | Ok uid ->
      let map fd ~pos off = Scheduler.inj (Memory.map fd ~pos off) in

      match Carton.Dec.Idx.find idx uid with
      | None -> IO.return (Ok None)
      | Some (_, offset) ->
        let fiber () =
          let ( >>= ) = scheduler.Carton.bind in
          Carton.Dec.path_of_offset scheduler ~map pack ~cursor:offset >>= fun path ->
          scheduler.Carton.return (Ok (Some (Object.is (Carton.Dec.kind_of_path path)))) in
        Scheduler.prj (fiber ())

  let get { idx; pack; _ } k =
    Object.digest k >>= function
    | Error (#Mirage_kv.error as err) -> IO.return (Error err)
    | Ok uid ->
      let map fd ~pos off = Scheduler.inj (Memory.map fd ~pos off) in

      match Carton.Dec.Idx.find idx uid with
      | None -> IO.return (Error (`Not_found k))
      | Some (_, offset) ->
        let fiber () =
          let ( >>= ) = scheduler.Carton.bind in
          let ( >>| ) x f = x >>= fun x -> scheduler.Carton.return (f x) in

          Carton.Dec.weight_of_offset scheduler ~map pack ~weight:Carton.Dec.null offset >>= fun weight ->
          Carton.Dec.path_of_offset scheduler ~map pack ~cursor:offset >>= fun path ->
          let raw = Carton.Dec.make_raw ~weight in
          Carton.Dec.of_offset_with_path scheduler ~map pack ~path raw ~cursor:offset >>| fun v ->
          Object.v ~kind:(Carton.Dec.kind v) (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v) in
        Scheduler.prj (fiber ()) >>= function
        | Ok _ as v -> IO.return v
        | Error (#Mirage_kv.error as err) -> IO.return (Error err)

  let list t k =
    let ( >>= ) = IO.bind in

    get t k >>= function
    | Ok v ->
      IO.return
        (Object.iter v |> function
          | Error (#Mirage_kv.error as err) -> Error err
          | Ok _ as v -> v)
    | Error (#Mirage_kv.error as err) -> IO.return (Error err)

  let last_modified _ _ = IO.return (Ok (0, 0L))
  (* XXX(dinosaure): on a read-only pack, elements don't have associated time. *)

  let digest { idx; _ } k =
    Object.digest k >>= function
    | Error (#Mirage_kv.error as err) -> IO.return (Error err)
    | Ok uid ->
      if Carton.Dec.Idx.exists idx uid
      then IO.return (Ok (Uid.to_raw_string uid))
      else IO.return (Error (`Not_found k))
end
