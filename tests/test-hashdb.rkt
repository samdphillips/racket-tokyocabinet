#lang racket/base

(require rackunit
         "../tokyocabinet/tc.rkt")

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

(test-case "tc-hdb-out"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (tc-hdb-put hdb #"key" #"1")
      (check-equal? (tc-hdb-get hdb #"key") #"1")
      (tc-hdb-out hdb #"key")
      (check-false (tc-hdb-get hdb #"key")))))

(test-case "tc-hdb-vsiz"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (check-false (tc-hdb-vsiz hdb #"key"))
      (tc-hdb-put hdb #"key" #"foo")
      (check-equal? (tc-hdb-vsiz hdb #"key") 3))))

(test-case "tc-hdb-iter-*"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (let ([keys (list #"a" #"b" #"c" #"d" #"e")])
        (for ([k (in-list keys)])
          (tc-hdb-put hdb k k))

        (tc-hdb-iter-init hdb)

        (let check ([keys keys])
          (if (null? keys)
              (check-false (tc-hdb-iter-next/key hdb))
              (let ([k (tc-hdb-iter-next hdb)])
                (check-not-false k)
                (check-not-false (member k keys))
                (check (remove k keys)))))

        ; this is for tchdbiternext3.  need support for TCXSTR
        #;
        (tc-hdb-iter-init hdb)
        #;
        (let check ([keys keys])
          (if (null? keys)
              (check-false (tc-hdb-iter-next/key+value hdb))
              (let-values ([(k v) (tc-hdb-iter-next/key+value hdb)])
                (check-not-false k)
                (check-not-false (member k keys))
                (check-not-false (member v keys))
                (check (remove k keys)))))))))

(test-case "tc-hdb-rnum"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (check-equal? (tc-hdb-rnum hdb) 0)

      (for ([k (list #"a" #"b" #"c")])
        (tc-hdb-put hdb k k))

      (check-equal? (tc-hdb-rnum hdb) 3))))

(test-case "tc-hdb-vanish"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (for ([k (list #"a" #"b" #"c")])
        (tc-hdb-put hdb k k))
      (check-equal? (tc-hdb-rnum hdb) 3)
      (tc-hdb-vanish hdb)
      (check-equal? (tc-hdb-rnum hdb) 0))))

(test-case "tc-hdb-copy"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb1)
      (let ([keys (list #"a" #"b" #"c")])
        (for ([k (in-list keys)])
          (tc-hdb-put hdb1 k k))
  
        (tc-hdb-copy hdb1 "test2.tch")
  
        (call-with-tc-hdb "test2.tch" #:options '(read)
          (lambda (hdb2)
            (check-equal? (tc-hdb-rnum hdb2) (tc-hdb-rnum hdb1))
            (for ([k (in-list keys)])
              (check-equal? (tc-hdb-get hdb2 k) (tc-hdb-get hdb1 k)))))))))
          

(test-case "tc-hdb-tran-*"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (tc-hdb-tran-begin hdb)
      (check-equal? (tc-hdb-rnum hdb) 0)
      (tc-hdb-put hdb #"a" #"b")
      (check-equal? (tc-hdb-rnum hdb) 1)
      (tc-hdb-tran-commit hdb)
      (check-equal? (tc-hdb-rnum hdb) 1)

      (tc-hdb-tran-begin hdb)
      (tc-hdb-put hdb #"c" #"d")
      (check-equal? (tc-hdb-rnum hdb) 2)
      (tc-hdb-tran-abort hdb)
      (check-equal? (tc-hdb-rnum hdb) 1))))

(test-case "tc-hdb-path"
  (call-with-tc-hdb "test.tch" #:options '(write truncate)
    (lambda (hdb)
      (check-equal? (tc-hdb-path hdb) "test.tch"))))
  
      
;; bool tchdbsync(TCHDB *hdb)


