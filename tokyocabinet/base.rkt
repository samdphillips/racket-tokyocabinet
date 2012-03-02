#lang racket/base

(require (for-syntax racket/base
                     racket/syntax)
         racket/stxparam
         
         ffi/unsafe
         ffi/file
         ffi/unsafe/alloc
         (only-in racket/dict 
                  dict-ref))

(define libtc
  (ffi-lib "libtokyocabinet"))

(define-syntax-parameter tc-proc-name 
  (lambda (stx)
    (raise-syntax-error #f "cannot use outside of define-tc" stx)))

(define-syntax define-tc
  (syntax-rules ()
    [(_ id type #:wrap wrap)
     (define id
       (syntax-parameterize ([tc-proc-name (lambda (stx)
                                             (syntax (quote id)))])
         (wrap (get-ffi-obj (regexp-replaces 'id '((#rx"-" "")))
                            libtc type))))]
    [(_ id type)
     (define-tc id type #:wrap values)]))

(define _tc-omode
  (_bitmask
   '(read     = #b1
              write    = #b10
              create   = #b100
              truncate = #b1000
              nolock   = #b10000
              noblock  = #b100000
              tsync    = #b1000000)))

;; Convert the _tc-omode into security-guard permissions
(define (mode->perms mode)
  (define m->p
    '([read     read]
      [write    read write]
      [create   read write]
      [truncate read write]))
  (for*/fold ([perms null]) ([m (in-list mode)]
                             [v (in-list (dict-ref m->p m null))])
    (if (memq v perms) perms (cons v perms))))

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

(define-syntax (define-tc-file-type stx)
  (syntax-case stx ()
    [(_ tyname)
     (let ([fmt (lambda (s)
                  (format-id #'tyname s #'tyname))])     
       (with-syntax ([_tyname            (fmt "_~a")]
                     [tyname-handle      (fmt "~a-handle")]
                     [tyname-new         (fmt "~a-new")]
                     [tyname-del         (fmt "~a-del")]
                     [set-tyname-handle! (fmt "set-~a-handle!")])
         #'(begin
             (struct tyname (handle) #:mutable)
             
             (define _tyname
               (make-ctype _pointer
                           (lambda (x)
                             (or (tyname-handle x)
                                 (error 'tyname "disposed ~a handle" 'tyname)))
                           tyname))
             
             (define-tc tyname-del
               (_fun (handle : _tyname)
                     -> _void
                     -> (set-tyname-handle! handle #f))
               #:wrap (deallocator))
                          
             (define-tc tyname-new
               (_fun -> _tyname)
               #:wrap (allocator tyname-del))
             
             )))]))

(define-syntax define-check-type
  (syntax-rules ()
    [(_ name err)
     (define-fun-syntax name
       (syntax-rules ()
         [(_ what)
          (name (tc-proc-name) what)]
         [(_ who what)
          (type: _bool
                 post: (result => (unless result
                                    (err who what))))]))]))

(define ((tc-open-wrapper who) f)
  (lambda (h p [mode '(read write create)])
    (security-guard-check-file
     who p (mode->perms mode))
    (f h p mode)))

(define-tc tc-version _string)

(provide (all-defined-out))