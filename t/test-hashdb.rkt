#lang planet samdphillips/testy
"Test Hash DB lib"

(require "../tokyocabinet/tc.rkt")

(test-case "tc-hdb-new/tc-hdb-del"
  (let ([hdb (tc-hdb-new)])
    (check-pred tc-hdb? hdb)
    (tc-hdb-del hdb)))

(test-case "tc-hdb-ecode/tc-hdb-errmsg"
  (let* ([hdb    (tc-hdb-new)]
         [status (tc-hdb-ecode hdb)])
    (check-equal? "success" (tc-hdb-errmsg status))))

(test-case "tc-hdb-open"
  (let ([hdb (tc-hdb-new)])
    (tc-hdb-open hdb "test.tch" '(read write create truncate))
    (tc-hdb-close hdb)
    (tc-hdb-del hdb)))

(test-case "tc-hdb-put/tc-hdb-get"
  (let ([hdb (tc-hdb-new)])
    (tc-hdb-open hdb "test.tch" '(read write create truncate))
    (tc-hdb-put hdb #"key" #"value")
    (check-equal? (tc-hdb-get hdb #"key") #"value")
    (tc-hdb-close hdb)
    (tc-hdb-del   hdb)))

(test-case "tc-hdb-get missing"
  (let ([hdb (tc-hdb-new)])
    (tc-hdb-open hdb "test.tch")
    (check-false (tc-hdb-get hdb #"wha?"))
    (check-equal? (tc-hdb-errmsg (tc-hdb-ecode hdb)) "no record found")
    (check-equal? (tc-hdb-ecode hdb) 'no-record)
    (tc-hdb-close hdb)
    (tc-hdb-del hdb)))
    
(test-case "tc-hdb-put twice"
  (let ([hdb (tc-hdb-new)])
    (tc-hdb-open hdb "test.tch" '(write truncate))
    (check-false (tc-hdb-get hdb #"key"))
    (tc-hdb-put hdb #"key" #"first value")
    (tc-hdb-put hdb #"key" #"second value")
    (check-equal? (tc-hdb-get hdb #"key") #"second value")
    (tc-hdb-close hdb)
    (tc-hdb-del hdb)))

(test-case "tc-hdb-put-keep"
  (let ([hdb (tc-hdb-new)])
    (tc-hdb-open hdb "test.tch" '(write truncate))
    (tc-hdb-put hdb #"key" #"value1")
    (check-exn exn?
               (lambda ()
                 (tc-hdb-put-keep hdb #"key" #"value2")))
    (check-equal? (tc-hdb-get hdb #"key") #"value1")
    (tc-hdb-close hdb)
    (tc-hdb-del hdb)))

(test-case "tc-hdb-put-cat"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (tc-hdb-put hdb #"key" #"1")
      (tc-hdb-put-cat hdb #"key" #"2")
      (check-equal? (tc-hdb-get hdb #"key") #"12"))))

