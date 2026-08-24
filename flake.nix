{
  description = "T3 Code CLI";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    nixpkgs-darwin.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";

    upstream-t3code = {
      url = "github:pingdotgg/t3code/f0ebc628c6dd83fd0c7963078ad7778ce6028d0c";
      flake = false;
    };
  };

  outputs =
    {
      nixpkgs,
      nixpkgs-darwin,
      self,
      upstream-t3code,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      nixpkgsFor = system: if system == "x86_64-darwin" then nixpkgs-darwin else nixpkgs;
    in
    {
      packages = nixpkgs.lib.genAttrs systems (
        system:
        let
          pkgs = import (nixpkgsFor system) { inherit system; };
        in
        rec {
          t3code-cli = pkgs.callPackage ./nix/package.nix {
            src = self;
            upstreamSrc = upstream-t3code;
          };

          default = t3code-cli;
        }
      );

      formatter = nixpkgs.lib.genAttrs systems (
        system:
        let
          pkgs = import (nixpkgsFor system) { inherit system; };
        in
        pkgs.nixfmt
      );
    };
}
