let cpu () =
  match String.lowercase_ascii (Sys.os_type) with
  | "win32" -> Sys.getenv "NUMBER_OF_PROCESSORS" |> int_of_string
  | "freebsd" ->
    let i = Unix.open_process_in "sysctl -n hw.ncpu" in
    let close () = ignore (Unix.close_process_in i) in
    ( try let i = Scanf.Scanning.from_channel i in
        Scanf.bscanf i "%d" (fun n -> close () ; n) with exn -> close () ; raise exn )
  | _ ->
    let i = Unix.open_process_in "getconf _NPROCESSORS_ONLN" in
    let close () = ignore (Unix.close_process_in i) in
    ( try let i = Scanf.Scanning.from_channel i in
        Scanf.bscanf i "%d" (fun n -> close () ; n) with exn -> close () ; raise exn )

let cpu = max (cpu ()) 3
