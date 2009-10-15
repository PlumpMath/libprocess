#ifndef PROCESS_HPP
#define PROCESS_HPP

#include <stdlib.h>
#include <ucontext.h>

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

class Process {
private:
  friend class LinkManager;
  friend class ProcessManager;
  friend void * schedule(void *arg);

  /* Mutex protecting process. */
  pthread_mutex_t mutex;

  /* Flag indicating state of process. */
  enum { READY,
	 RUNNING,
	 RECEIVING,
	 PAUSED,
	 AWAITING,
	 TIMEDOUT,
	 WAITING,
	 EXITED } state;

  /* Queue of messages received. */
  std::queue<struct msg *> msgs;

  /* Current message. */
  struct msg *current;

  /* Current "blocking" generation. */
  int generation;

  /* Process PID. */
  PID pid;

  /* Continuation/Context of process. */
  ucontext_t uctx;

  /* Enqueues the specified message. */
  void enqueue(struct msg &msg);

  /* Dequeues a message or returns NULL. */
  struct msg * dequeue();
  
protected:
  Process();

  /* Function run when process spawned. */
  virtual void operator() () = 0;

  /* Returns the PID describing this process. */
  PID self();

  /* Returns the sender's PID of the last dequeued (current) message. */
  PID from();

  MSGID msgid() { return current != NULL ? current->id : PROCESS_ERROR; }

  /* Sends a message to PID. */
  void send(const PID &to, MSGID id) { send(to, id, std::make_pair((char *) NULL, 0)); }

  /* Sends a message with data to PID. */
  virtual void send(const PID &to, MSGID id, const std::pair<const char *, size_t> &body);

  /* Blocks for message indefinitely. */
  MSGID receive() { return receive(0); }

  /* Blocks for message at most specified seconds. */
  virtual MSGID receive(time_t secs);

  /* Returns pointer and length of body of last dequeued (current) message. */
  virtual std::pair<const char *, size_t> body();

  /* Blocks at least specified seconds (may block longer). */
  virtual void pause(time_t secs);

  /* Links with the specified PID. */
  virtual PID link(const PID &to);

  /* IO operations for awaiting. */
  enum { RDONLY = 01, WRONLY = 02, RDWR = 03 };

  /* Wait until operation is ready for file descriptor. */
  virtual void await(int fd, int op);

  /* Returns true if operation on file descriptor is ready. */
  virtual bool ready(int fd, int op);

public:
  const PID getPID() { return self(); }

  /* Spawn a new process. */
  static PID spawn(Process *process);

  /* Wait for all the specified PID's to exit. */
  static void wait(const PID &pid);
};


#endif /* PROCESS_HPP */
