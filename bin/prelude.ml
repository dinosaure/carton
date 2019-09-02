module Mutex = struct
  type 'a fiber = 'a
  type t = Mutex.t

  let create () = Mutex.create ()
  let lock t = Mutex.lock t
  let unlock t = Mutex.unlock t
end

module Future = struct
  type 'a fiber = 'a
  type 'a t = { th : Thread.t; v: 'a option ref }

  let wait { th; v } =
    Thread.join th ; match !v with Some v -> v | None -> assert false
  let peek { v; _ } = !v
end

module Us = Cd.Make(struct type 'a t = 'a end)

let unix =
  { Cd.bind= (fun x f -> f (Us.prj x))
  ; Cd.return= (fun x -> Us.inj x) }

type fd = { fd : Unix.file_descr; mx : int; }

let unix_map : (fd, Us.t) Cd.W.map =
  fun fd ~pos len ->
  let payload =
    let len = min (fd.mx - pos) len in
    Mmap.V1.map_file fd.fd ~pos:(Int64.of_int pos)
      Bigarray.char Bigarray.c_layout false [| len |] in
  Us.inj (Bigarray.array1_of_genarray payload)

let unix_read : (in_channel, Us.t) Cd.read =
  fun fd buf ~off ~len ->
  let n = input fd buf off len in
  Us.inj n

module IO = struct
  type 'a t = 'a

  module Mutex = Mutex
  module Future = Future

  let bind x f = f x
  let return x = x

  let nfork_map l ~f = match l with
    | [] -> []
    | [ x ] ->
      let v = ref None in
      let th = Thread.create (fun () -> v := Some (f x)) () in
      [ { Future.th; Future.v; } ]
    | vs ->
      let f x =
        let v = ref None in
        let th = Thread.create (fun () -> v := Some (f x)) () in
        { Future.th; Future.v; } in
      List.map f vs

  let all_unit l = List.iter Sys.opaque_identity l
end
