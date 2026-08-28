#!/bin/sh
cargo bench --color=always --features "std,comparison-bench,solana" \
  --bench codec_bench --bench solana_bench 2>&1 \
  | grep --line-buffered -E 'rank' \
  | grep --line-buffered 'lencode' \
  | awk '{
      if ($0 ~ /^ *1st/) printf "\033[32m%s\033[0m\n", $0;
      else printf "\033[31m%s\033[0m\n", $0;
      fflush();
    }'
