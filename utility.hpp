#include <sstream>
#include <string>

std::string
operator+(const std::string &s, time_t t)
{
  std::stringstream out;
  out << t;
  return s + out.str();
}

