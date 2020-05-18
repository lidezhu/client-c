#find_package (Poco COMPONENTS Foundation REQUIRED)
#find_package (Poco COMPONENTS Net REQUIRED)

find_package (Poco REQUIRED Foundation Net Json Util)

#set(Poco_Foundation_LIBRARY PocoFoundation)
#set(Poco_Net_LIBRARY PocoNet)

message(STATUS "Using Poco: ${Poco_INCLUDE_DIRS} : ${Poco_Foundation_LIBRARY}, ${Poco_Net_LIBRARY},${Poco_VERSION}")
