#!/usr/bin/env ocaml
#use       "topfind";;
#require   "topkg";;

open Topkg

let () =
  Pkg.describe "carton" @@ fun c ->

  Ok [ Pkg.lib "pkg/META"
     ; Pkg.doc "README.md"
     ; Pkg.doc "CHANGES.md"
     ; Pkg.mllib "src/carton.mllib" ]
