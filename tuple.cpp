#include "tuple.hpp"

namespace process { namespace serialize {

void serializer::operator & (const ::boost::tuples::null_type &)
{
  abort();
}

}}
