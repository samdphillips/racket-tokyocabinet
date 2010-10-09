#lang planet samdphillips/testy
"Test Utility lib"

(require "../tokyocabinet/tc.rkt")

(test-case "Check version"
  (check-regexp-match "^1\\..*$" tc-version))


