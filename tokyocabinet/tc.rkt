#lang racket/base

(require "base.rkt")

;; TODO: make-sized-byte-string for returned values
;; TODO: inventory hdb api
;; TODO: docs

(require ffi/unsafe)

(define-tc-file-type tc-hdb)

(define-tc tc-hdb-ecode  (_fun _tc-hdb   -> _tc-ecode))
(define-tc tc-hdb-errmsg (_fun _tc-ecode -> _string))

(define (tc-hdb-error who hdb)
  (error who "tokyo cabinet error: ~a"
         (tc-hdb-errmsg (tc-hdb-ecode hdb))))

(define-check-type _hdb-result tc-hdb-error)

;; tchdbsetmutex
;; tchdbtune
;; tchdbsetcache

(define-tc tc-hdb-open  
  (_fun (hdb : _tc-hdb) _path _tc-omode -> (_hdb-result hdb))
  #:wrap (tc-open-wrapper (tc-proc-name)))

(define-tc tc-hdb-close 
  (_fun (hdb : _tc-hdb) 
        -> (_hdb-result hdb)))

(define (call-with-tc-hdb file #:options [opts '(write create)] proc)
  (call-with-continuation-barrier
   (lambda ()
     (let ([hdb (tc-hdb-new)])
       (dynamic-wind
        void
        (lambda ()
          (tc-hdb-open hdb file opts)
          (dynamic-wind
           void
           (lambda ()
             (proc hdb))
           (lambda ()
             (tc-hdb-close hdb))))
        (lambda ()
          (tc-hdb-del hdb)))))))

(define-tc tc-hdb-put
  (_fun (hdb : _tc-hdb) 
        (key : _bytes) (_int = (bytes-length key))
        (val : _bytes) (_int = (bytes-length val))
        -> (_hdb-result hdb)))

(define-tc tc-hdb-get
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        (size : (_ptr o _int))
        -> (val : _pointer)
        -> (cond [val (let ([v (make-bytes size)])
                        (memcpy v val size)
                        (free val)
                        v)]
                 [(eq? 'no-record (tc-hdb-ecode hdb)) #f]
                 [else
                  (tc-hdb-error (tc-proc-name) hdb)])))

(define-tc tc-hdb-put-keep
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        (val : _bytes) (_int = (bytes-length val))
        -> (_hdb-result hdb)))

(define-tc tc-hdb-put-cat
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        (val : _bytes) (_int = (bytes-length val))
        -> (_hdb-result hdb)))

(define-tc tc-hdb-out
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        -> (_hdb-result 'tc-hdb-out hdb)))

(define-tc tc-hdb-vsiz
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        -> (val : _int)
        -> (cond [(< val 0) #f]
                 [else val])))

(define-tc tc-hdb-iter-init
  (_fun (hdb : _tc-hdb)
        -> (_hdb-result hdb)))

(define-tc tc-hdb-iter-next
  (_fun (hdb : _tc-hdb)
        (size : (_ptr o _int))
        -> (key : _pointer)
        -> (if key
               (let ([k (make-bytes size)])
                 (memcpy k key size)
                 (free key)
                 k)
               #f)))


(define tc-hdb-iter-next/key       tc-hdb-iter-next)

;;; XXX: implement this in terms of iter-next and get
#;
(define tc-hdb-iter-next/key+value tc-hdb-iter-next3)

(define-tc tc-hdb-rnum
  (_fun (hdb : _tc-hdb)
        -> _uint64))

(define-tc tc-hdb-vanish
  (_fun (hdb : _tc-hdb)
        -> (_hdb-result hdb)))

;; XXX: check path with security guard
(define-tc tc-hdb-copy
  (_fun (hdb : _tc-hdb) _path
        -> (_hdb-result hdb)))

(define-tc tc-hdb-tran-begin
  (_fun (hdb : _tc-hdb)
        -> (_hdb-result hdb)))

(define-tc tc-hdb-tran-commit
  (_fun (hdb : _tc-hdb)
        -> (_hdb-result hdb)))

(define-tc tc-hdb-tran-abort
  (_fun (hdb : _tc-hdb)
        -> (_hdb-result hdb)))

;; XXX: should this return a _path ?
(define-tc tc-hdb-path
  (_fun (hdb : _tc-hdb)
        -> _string))

(provide (all-defined-out))
