#ifndef TUPLE_HPP
#define TUPLE_HPP

#include <sstream>
#include <string>
#include <utility>

#include <boost/tuple/tuple.hpp>

#include <boost/variant/variant.hpp>
#include <boost/variant/get.hpp>

#include "process.hpp"
#include "serialization.hpp"

using process::serialization::serializer;
using process::serialization::deserializer;

namespace process { namespace serialization {

void operator & (serializer &, const ::boost::tuples::null_type &);

}}

namespace process { namespace tuple {
  
template <MSGID ID> struct tuple;

#define IDENTITY(...) __VA_ARGS__

#define TUPLE(ID, types)                           \
template <> struct tuple<ID>                       \
{                                                  \
   typedef ::boost::tuple<IDENTITY types> type;    \
   mutable type t;                                 \
   tuple(const type &_t) : t(_t) {}                \
}

const ::boost::tuples::detail::swallow_assign _ = ::boost::tuples::ignore;

template <MSGID ID>
struct size {
  static const int value = ::boost::tuples::length<typename tuple<ID>::type>::value;
};

template <bool b, int n, MSGID ID>
struct field_impl
{
  typedef typename ::boost::tuples::element<n, typename tuple<ID>::type>::type type;
};

template <int n, MSGID ID>
struct field_impl<false, n, ID>
{
  typedef ::boost::tuples::null_type type;
};

template <int n, MSGID ID>
struct field
{
  typedef typename field_impl<n < size<ID>::value, n, ID>::type type;
};

template <bool b, int n, MSGID ID>
struct __at
{
  static typename field<n, ID>::type impl(const tuple<ID> &r)
  {
    return ::boost::tuples::get<n>(r.t);
  }
};

template <int n, MSGID ID>
struct __at<false, n, ID>
{
  static ::boost::tuples::null_type impl(const tuple<ID> &r)
  {
    return ::boost::tuples::null_type();
  }
};

template <int n, MSGID ID>
typename field<n, ID>::type
at(const tuple<ID> &r)
{
  return __at<n < size<ID>::value, n, ID>::impl(r);
}

template <typename P>
class Tuple : public P
{
protected:
  template <MSGID ID>
  tuple<ID> pack()
  {
    return tuple<ID>(::boost::make_tuple());
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0)
  {
    return tuple<ID>(::boost::make_tuple(t0));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4,
		 typename field<5, ID>::type t5)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4, t5));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4,
		 typename field<5, ID>::type t5,
		 typename field<6, ID>::type t6)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4, t5, t6));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4,
		 typename field<5, ID>::type t5,
		 typename field<6, ID>::type t6,
		 typename field<7, ID>::type t7)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4, t5, t6, t7));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4,
		 typename field<5, ID>::type t5,
		 typename field<6, ID>::type t6,
		 typename field<7, ID>::type t7,
		 typename field<8, ID>::type t8)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4, t5, t6, t7, t8));
  }

  template <MSGID ID>
  tuple<ID> pack(typename field<0, ID>::type t0,
		 typename field<1, ID>::type t1,
		 typename field<2, ID>::type t2,
		 typename field<3, ID>::type t3,
		 typename field<4, ID>::type t4,
		 typename field<5, ID>::type t5,
		 typename field<6, ID>::type t6,
		 typename field<7, ID>::type t7,
		 typename field<8, ID>::type t8,
		 typename field<9, ID>::type t9)
  {
    return tuple<ID>(::boost::make_tuple(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9));
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
  }


  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
  }


  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4,
	      typename field<5, ID>::type &t5)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
    d & t5;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4,
	      typename field<5, ID>::type &t5,
	      typename field<6, ID>::type &t6)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
    d & t5;
    d & t6;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4,
	      typename field<5, ID>::type &t5,
	      typename field<6, ID>::type &t6,
	      typename field<7, ID>::type &t7)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
    d & t5;
    d & t6;
    d & t7;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4,
	      typename field<5, ID>::type &t5,
	      typename field<6, ID>::type &t6,
	      typename field<7, ID>::type &t7,
	      typename field<8, ID>::type &t8)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
    d & t5;
    d & t6;
    d & t7;
    d & t8;
  }

  template <MSGID ID>
  void unpack(typename field<0, ID>::type &t0,
	      typename field<1, ID>::type &t1,
	      typename field<2, ID>::type &t2,
	      typename field<3, ID>::type &t3,
	      typename field<4, ID>::type &t4,
	      typename field<5, ID>::type &t5,
	      typename field<6, ID>::type &t6,
	      typename field<7, ID>::type &t7,
	      typename field<8, ID>::type &t8,
	      typename field<9, ID>::type &t9)
  {
    std::pair<const char *, size_t> b = Process::body();
    std::string data(b.first, b.second);

    std::istringstream is(data);

    deserializer d(is);

    d & t0;
    d & t1;
    d & t2;
    d & t3;
    d & t4;
    d & t5;
    d & t6;
    d & t7;
    d & t8;
    d & t9;
  }

  template <MSGID ID>
  void send(const PID &to, const tuple<ID> &r)
  {
    std::ostringstream os;

    serializer s(os);

    if (size<ID>::value >= 1) {
      const typename field<0, ID>::type &temp = at<0>(r);
      s & temp;
    }

    if (size<ID>::value >= 2) {
      const typename field<1, ID>::type &temp = at<1>(r);
      s & temp;
    }
 
    if (size<ID>::value >= 3) {
      const typename field<2, ID>::type &temp = at<2>(r);
      s & temp;
    }

    if (size<ID>::value >= 4) {
      const typename field<3, ID>::type &temp = at<3>(r);
      s & temp;
    }

    if (size<ID>::value >= 5) {
      const typename field<4, ID>::type &temp = at<4>(r);
      s & temp;
    }

    if (size<ID>::value >= 6) {
      const typename field<5, ID>::type &temp = at<5>(r);
      s & temp;
    }

    if (size<ID>::value >= 7) {
      const typename field<6, ID>::type &temp = at<6>(r);
      s & temp;
    }

    if (size<ID>::value >= 8) {
      const typename field<7, ID>::type &temp = at<7>(r);
      s & temp;
    }

    if (size<ID>::value >= 9) {
      const typename field<8, ID>::type &temp = at<8>(r);
      s & temp;
    }

    if (size<ID>::value >= 10) {
      const typename field<9, ID>::type &temp = at<9>(r);
      s & temp;
    }

    std::string data = os.str();

    Process::send(to, ID, std::make_pair(data.data(), data.size()));
  }

  template <MSGID ID>
  void send(const PID &to)
  {
    return send(to, pack<ID>());
  }

  template <MSGID ID>
  void send(const PID &to, typename field<0, ID>::type t0)
  {
    send(to, pack<ID>(t0));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1)
  {
    send(to, pack<ID>(t0, t1));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2)
  {
    send(to, pack<ID>(t0, t1, t2));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3)
  {
    send(to, pack<ID>(t0, t1, t2, t3));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4,
	    typename field<5, ID>::type t5)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4, t5));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4,
	    typename field<5, ID>::type t5,
	    typename field<6, ID>::type t6)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4, t5, t6));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4,
	    typename field<5, ID>::type t5,
	    typename field<6, ID>::type t6,
	    typename field<7, ID>::type t7)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4, t5, t6, t7));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4,
	    typename field<5, ID>::type t5,
	    typename field<6, ID>::type t6,
	    typename field<7, ID>::type t7,
	    typename field<8, ID>::type t8)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4, t5, t6, t7, t8));
  }

  template <MSGID ID>
  void send(const PID &to,
	    typename field<0, ID>::type t0,
	    typename field<1, ID>::type t1,
	    typename field<2, ID>::type t2,
	    typename field<3, ID>::type t3,
	    typename field<4, ID>::type t4,
	    typename field<5, ID>::type t5,
	    typename field<6, ID>::type t6,
	    typename field<7, ID>::type t7,
	    typename field<8, ID>::type t8,
	    typename field<9, ID>::type t9)
  {
    send(to, pack<ID>(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9));
  }

  MSGID receive()
  {
    return receive(0);
  }

  MSGID receive(time_t secs)
  {
    Process::receive(secs);
  }

  template <MSGID ID>
  void receive(const ::boost::variant<
	       typename field<0, ID>::type &,
	       const typename field<0, ID>::type &,
	       const ::boost::tuples::detail::swallow_assign &> &v0)
  {
    if (typename field<0, ID>::type *t =
	::boost::get<typename field<0, ID>::type &>(&v0)) {
      // TODO(benh): Set this formal.
    } else if (const typename field<0, ID>::type *t =
	       ::boost::get<const typename field<0, ID>::type &>(&v0)) {
      // TODO(benh): Match this actual.
    } else {
      // TODO(benh): Ignore this parameter.
    }
  }
};

}}




#endif /* TUPLE_HPP */

