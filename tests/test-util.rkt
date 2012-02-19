#lang racket/base

(require rackunit
         "../tokyocabinet/tc.rkt")

(test-case "Check version"
  (check-regexp-match "^1\\..*$" tc-version))

