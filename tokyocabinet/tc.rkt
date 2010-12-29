#lang racket

(require ffi/unsafe)

(define libtc
  (ffi-lib "libtokyocabinet"))

(define-syntax define-tc
  (syntax-rules ()
    [(_ id type #:wrap w)
     (define id
       (let ([f (get-ffi-obj (regexp-replaces 'id '((#rx"-" "")))
                             libtc type)])
         (w f)))]
    [(_ id type)
     (define-tc id type #:wrap values)]))

(define-syntax define-check-type
  (syntax-rules ()
    [(_ name checkf)
     (define-fun-syntax name
       (syntax-rules ()
         [(_ who what)
          (type: _bool
           post: (r => (checkf who what r)))]))]))

(define _omode
  (_bitmask
    '(read     = #b1
      write    = #b10
      create   = #b100
      truncate = #b1000
      nolock   = #b10000
      noblock  = #b100000)))

(define _tc-ecode
  (_enum
    '(success
      thread-error
      invalid-op
      no-file
      no-perm
      invalid-metadata
      invalid-record-header
      open-error
      close-error
      truncate-error
      sync-error
      stat-error
      seek-error
      read-error
      write-error
      mmap-error
      lock-error
      unlink-error
      rename-error
      mkdir-error
      rmdir-error
      existing-record
      no-record
      misc-error = 9999)))

(define-cpointer-type _tc-hdb)

;;; FIXME: probably should categorize kinds of exceptions
(define (check-hdb-error who hdb result)
  (unless result
    (hdb-error who hdb)))

(define (hdb-error who hdb)
  (error who "tokyo cabinet error: ~a"
         (tc-hdb-errmsg (tc-hdb-ecode hdb))))

(define-check-type _hdb-result check-hdb-error)

(define-tc tc-version    _string)
(define-tc tc-hdb-new    (_fun           -> _tc-hdb))
(define-tc tc-hdb-del    (_fun _tc-hdb   -> _void))
(define-tc tc-hdb-ecode  (_fun _tc-hdb   -> _tc-ecode))
(define-tc tc-hdb-errmsg (_fun _tc-ecode -> _string))

;; tchdbsetmutex
;; tchdbtune
;; tchdbsetcache

(define-tc tc-hdb-open  
  (_fun (hdb : _tc-hdb) _path _omode 
        -> (_hdb-result 'tc-hdb-open hdb))
  #:wrap (lambda (f)
           (lambda (h p [mode '(read write create)])
             (f h p mode))))

(define-tc tc-hdb-close 
  (_fun (hdb : _tc-hdb) 
        -> (_hdb-result 'tc-hdb-close hdb)))

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
        -> (_hdb-result 'tc-hdb-put hdb)))

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
                   (hdb-error 'tc-hdb-get hdb)])))

(define-tc tc-hdb-put-keep
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        (val : _bytes) (_int = (bytes-length val))
        -> (_hdb-result 'tc-hdb-put-keep hdb)))

(define-tc tc-hdb-put-cat
  (_fun (hdb : _tc-hdb)
        (key : _bytes) (_int = (bytes-length key))
        (val : _bytes) (_int = (bytes-length val))
        -> (_hdb-result 'tc-hdb-put-cat hdb)))

(provide (all-defined-out))
