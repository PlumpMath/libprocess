#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <sstream>
#include <string>
#include <utility>


namespace process { namespace serialize {

struct serializer
{
  std::ostringstream& stream;

  serializer(std::ostringstream& s) : stream(s) {}

  void operator & (const int32_t &);
  void operator & (const int64_t &);
  void operator & (const size_t &);
  void operator & (const std::string &);
  void operator & (const PID &);
};

struct deserializer
{
  std::istringstream &stream;

  deserializer(std::istringstream &s) : stream(s) {}

  void operator & (int32_t &);
  void operator & (int64_t &);
  void operator & (size_t &);
  void operator & (std::string &);
  void operator & (PID &);
};

}}

#endif /* SERIALIZE_HPP */
