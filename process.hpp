#ifndef PROCESS_HPP
#define PROCESS_HPP

#include <stdlib.h>
#include <ucontext.h>

#ifdef USE_LITHE
#include <lithe.hh>

#include <ht/ht.h>
#include <ht/spinlock.h>
#endif /* USE_LITHE */

#include <iostream>
#include <queue>
#include <string>
#include <utility>

typedef uint16_t MSGID;

const MSGID PROCESS_ERROR = 0;
const MSGID PROCESS_TIMEOUT = 1;
const MSGID PROCESS_EXIT = 2;
const MSGID PROCESS_MSGID = PROCESS_EXIT+1;

typedef struct PID
{
  uint32_t pipe;
  uint32_t ip;
  uint16_t port;

  operator std::string() const;
  bool operator ! () const;
} PID;

std::ostream& operator << (std::ostream& stream, const PID& pid);
std::istream& operator >> (std::istream& stream, PID& pid);

bool operator < (const PID &left, const PID &right);
bool operator == (const PID &left, const PID &right);

PID make_pid(const char *);

struct msg
{
  PID from;
  PID to;
  MSGID id;
  uint32_t len;
};


#ifdef USE_LITHE

using lithe::Scheduler;

class ProcessScheduler : public Scheduler
{
private:
  lithe_task_t task;

protected:
  void enter();
  void yield(lithe_sched_t *child);
  void reg(lithe_sched_t *child);
  void unreg(lithe_sched_t *child);
  void request(lithe_sched_t *child, int k);
  void unblock(lithe_task_t *task);

public:
  ProcessScheduler();
  ~ProcessScheduler();
};

#endif /* USE_LITHE */


void * schedule(void *arg);


class Process {
private:
  friend class LinkManager;
  friend class ProcessManager;
#ifdef USE_LITHE
  friend class ProcessScheduler;
#else
  friend void * schedule(void *arg);
#endif /* USE_LITHE */

  /* Flag indicating state of process. */
  enum { INIT,
	 READY,
	 RUNNING,
	 RECEIVING,
	 PAUSED,
	 AWAITING,
	 WAITING,
	 INTERRUPTED,
	 TIMEDOUT,
	 EXITED } state;

  /* Queue of messages received. */
  std::queue<struct msg *> msgs;

  /* Current message. */
  struct msg *current;

  /* Current "blocking" generation. */
  int generation;

  /* Process PID. */
  PID pid;

#ifdef USE_LITHE
  lithe_task_t task;
#endif /* USE_LITHE */

  /* Continuation/Context of process. */
  ucontext_t uctx;

  /* Lock/mutex protecting internals. */
#ifdef USE_LITHE
  int l;
  void lock() { spinlock_lock(&l); }
  void unlock() { spinlock_unlock(&l); }
#else
  pthread_mutex_t m;
  void lock() { pthread_mutex_lock(&m); }
  void unlock() { pthread_mutex_unlock(&m); }
#endif /* USE_LITHE */

  /* Enqueues the specified message. */
  void enqueue(struct msg *msg);

  /* Dequeues a message or returns NULL. */
  struct msg * dequeue();

#ifdef SWIGPYTHON
public:
#else  
protected:
#endif /* SWIG */

  Process();

  /* Function run when process spawned. */
  virtual void operator() () = 0;

  /* Returns the PID describing this process. */
  PID self();

  /* Returns the sender's PID of the last dequeued (current) message. */
  PID from();

  MSGID msgid();

  /* Sends a message to PID. */
  void send(const PID &, MSGID);

  /* Sends a message with data to PID. */
  void send(const PID &, MSGID, const char *data, size_t length);

  /* Blocks for message indefinitely. */
  MSGID receive();

  /* Blocks for message at most specified seconds. */
  MSGID receive(time_t);

  /* Returns pointer and length of body of last dequeued (current) message. */
  const char * body(size_t *length);

  /* Blocks at least specified seconds (may block longer). */
  void pause(time_t);

  /* Links with the specified PID. */
  PID link(const PID &);

  /* IO operations for awaiting. */
  enum { RDONLY = 01, WRONLY = 02, RDWR = 03 };

  /* Wait until operation is ready for file descriptor (or message received). */
  bool await(int fd, int op);

  /* Returns true if operation on file descriptor is ready. */
  bool ready(int fd, int op);

public:
  virtual ~Process();

  /* Returns pid of process; valid even before calling spawn. */
  PID getPID();

  /* Spawn a new process. */
  static PID spawn(Process *process);

  /* Wait for PID to exit (returns true if actually waited). */
  static bool wait(PID pid);
};


inline MSGID Process::msgid()
{
  return current != NULL ? current->id : PROCESS_ERROR;
}


inline void Process::send(const PID &to, MSGID id)
{
  send(to, id, NULL, 0);
}


inline MSGID Process::receive()
{
  return receive(0);
}


inline PID Process::getPID()
{
  return self();
}

#endif /* PROCESS_HPP */
