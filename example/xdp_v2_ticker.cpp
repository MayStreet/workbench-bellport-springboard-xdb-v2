//--- bp/example/xdp_v2_ticker.cpp ---------------------------------*- C++ -*-==//
//
//               Bellport Low Latency Trading Infrastructure.
//
// Copyright MayStreet 2021 - all rights reserved
//
// $Id: 7ab095df5757913ea72685fc9d6aa7795be7f79a $
//===---------------------
#include <cstddef>
#include <vector>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <signal.h>
#include <type_traits>

#include <bp/core/program_options.hpp>
#include <bp/core/config_tree.hpp>
#include <bp/core/serialization.hpp>

#include <bp/feed/xdp_v2/xdp_v2.hpp>

#include <bp/device/pcap_multi_file_device.hpp>
#include <bp/device/selector_recv_device.hpp>
#include <bp/device/serialization.hpp>

#include <bp/sys/keyboard.hpp>
#include <bp/sys/stringutils.hpp>
#include <bp/sys/time.hpp>
#include <bp/sys/processor_timer.hpp>

#include <bp/feed/default_feed_configuration.hpp>
#include <bp/feed/serialization.hpp>
#include <bp/feed/feed_configuration_filter.hpp>

bool g_loop = true;

namespace {
std::string GetFeedsAsStringInner(std::string delim)
{
  std::stringstream ss;
  auto feed_names = bp::feed::xdp_v2::FeedSettings::FeedNames();
  if(feed_names.empty())
  {
    return std::string();
  }
  auto it = feed_names.begin();
  ss << *it;
  ++it;
  while(it != feed_names.end())
  {
    ss << delim;
    ss << *it;
    ++it;
  }
  return ss.str();
}
std::string GetFeedsAsString(std::string delim = ", ",
                             std::string pre = std::string(),
                             std::string post = std::string())
{
  std::stringstream ss;
  ss << pre;
  ss << GetFeedsAsStringInner(delim);
  ss << post;
  return ss.str();
}
}

void Usage() {
  std::cout << "Usage: xdp_v2_ticker [ARGS]"                                                               << std::endl;
  std::cout << "Data is supplied from pcap files."                                                         << std::endl;
  std::cout                                                                                                << std::endl;
  std::cout << " --products <p1 p2 p3>  A list of products to monitor"                                     << std::endl;
  std::cout << " --license-file <file>  path to a license file.  "                                         << std::endl;
  std::cout << " --feed-name <name>     The feed that you want to connect to.  Possible values are:"       << std::endl;
  std::cout << "                        " << GetFeedsAsString(", ", "[","]")                               << std::endl;
  std::cout << " --cfg <cfg_file>       Use the configuration file for feed setup"                         << std::endl;
  std::cout << "                        ex: xdp_v2_ticker.cfg"                                             << std::endl;
  std::cout << " --pcaps <dir/path/glob> path or paths, with support for * in globbing"                    << std::endl;
  std::cout << " --symbol-map-file      Symbol Mapping File"                                               << std::endl;
  std::cout << " --series-map-file      Series index Mapping File"                                         << std::endl;
  std::cout << " -h --help              Print this message"                                                << std::endl;
  std::cout                                                                                                << std::endl;
  std::cout                                                                                                << std::endl;
  std::cout << "Example to run from pcap:"                                                                 << std::endl;
  std::cout                                                                                                << std::endl;
  std::cout << "  xdp_v2_ticker --pcaps /tmp/2014-03-27/*.pcap --symbol-map-file ~/SymbolMap.xml --cfg ../xdp_v2_ticker.cfg --products IBM" << std::endl;
}


// A message handler that prints to stdout when certain messages are received. Further business logic to be executed
// for each message type would go here.
class TickerPrinter : public bp::feed::AggregatedPriceFeedCallbackAdapter {
public:
  TickerPrinter() {}
  ~TickerPrinter(){}

  virtual void OnPacketBegin(const bp::feed::PacketBegin& packet_begin, void* /*closure*/) {
    BP_UNUSED(packet_begin);
  }


  virtual void OnMissingPackets(const bp::feed::MissingPackets& missing_packets, void* /*closure*/) {
    std::cout << "[" << missing_packets.session()->instance_name()
              << "] Missing packets: " << missing_packets.expected_sequence_number()
              << " < " << missing_packets.sequence_number() << " [Expected < Current]"
              << std::endl;
  }

  virtual void OnError(const bp::feed::Error& error, void* /*closure*/) {
    std::cout << error.ToString() << std::endl;
    assert(false && "error in ticker printer");
  }

  virtual void OnAggregatedPriceUpdate(const bp::feed::AggregatedPriceUpdate& apu, void* /*closure*/) {
    printf("%s %8s |%10" PRId64 " %10" PRId64 " |%6" PRId64 " %6" PRId64 " [%6s] %s\n"
              , apu.last_receipt_timestamp().ToString("%Y/%m/%d %H:%M:%S:%6N").c_str()
              , bp::feed::BookStatusToString(apu.book_status())
              , apu.first_sequence_number()
              , apu.last_sequence_number()
              , apu.first_product_sequence_number()
              , apu.last_product_sequence_number()
              , apu.product().name().c_str()
              , apu.top_of_book().ToString().c_str());
  }

  virtual void OnBBOQuote(const bp::feed::BBOQuote& bbo_quote, void* closure) {
    printf("%s: BBO   %s %s %6s %17s %8" PRIu64 " |   bid: %6" PRIu64 " @ %6s | ask: %6" PRIu64 " @ %6s |\n",
      (const char*)closure,
      bbo_quote.receipt_timestamp().ToString().c_str(),
      bbo_quote.exchange_timestamp().ToString().c_str(),
      bbo_quote.product().name().c_str(),
      bp::feed::MarketParticipantToString(bbo_quote.update_trigger_participant()),
      bbo_quote.sequence_number(),
      bbo_quote.bid_quantity(),
      bbo_quote.bid_quantity()==0?"N/A":bbo_quote.bid_price().ToString().c_str(),
      bbo_quote.ask_quantity(),
      bbo_quote.ask_quantity()==0?"N/A":bbo_quote.ask_price().ToString().c_str());
  }
  virtual void OnTrade(const bp::feed::Trade& trade, void* /*closure*/) {
    printf("TRADE: %s %8s %8s %10" PRId64 " %6" PRId64 " [%6s] % 4" PRId64 " @ %6s\n"
              , trade.receipt_timestamp().ToString("%Y/%m/%d %H:%M:%S:%9N").c_str()
              , trade.exchange_timestamp().ToString("%Y/%m/%d %H:%M:%S:%9N").c_str()
              , bp::feed::BookStatusToString(trade.book_status())
              , trade.sequence_number()
              , trade.product_sequence_number()
              , trade.product().name().c_str()
              , trade.quantity()
              , trade.price().ToString().c_str());

  }
  virtual void OnProductAnnouncement(const bp::feed::ProductAnnouncement& product_announcement, void* /*closure*/) {
    //bp::feed::xdp_v2::AggregatedPriceFeedSession* session;
    //session = (bp::feed::xdp_v2::AggregatedPriceFeedSession*)bp::feed::CurrentAggregatedPriceSession::Get();
    //assert(session);
    //const bp::feed::xdp_v2::SymbolMap* sm = session->symbol_mapping_reader().GetSymbolMap(product_announcement.product().name());
    //uint64_t price_scale_code = sm->price_scale_code;
    std::cout << product_announcement.ToString() << std::endl;
  }
  virtual void OnProductStatus(const bp::feed::ProductStatus& /*product_status*/, void* /*closure*/) {
    //std::cout << "PRODUCT STATUS : " << product_status.ToString() << std::endl;
  }
};

template <typename FEED_TYPE>
bp::core::Result SetupFeed(const std::string& feed_name,
                           const char* cfg_arg,
                           bp::device::PcapMultiFileDevice& pcap_device,
                           bp::feed::IAggregatedPriceFeedCallback* callback,
                           void* closure,
                           const std::vector<std::string>& products_arg,
                           const char* license_file_arg,
                           const char* symbol_map_file_arg,
                           const char* series_map_file_arg,
                           FEED_TYPE* feed)
{
  bp::core::Result bpResult;



  // Load in the configuration for this feed:
  // This type is actually something like bp::feed::cqs::FeedSettings
  typename FEED_TYPE::SettingsType settings;
  std::string cfg;
  if (cfg_arg) {
    cfg = cfg_arg;
  }
  if (!(bpResult = bp::feed::LoadFeedSettings(feed_name, cfg, &settings))) {
    BP_LOG_ERROR() << "LoadFeedSettings failed with error " << bpResult.ToString();
    return bpResult;
  }
  if (NULL != license_file_arg) {
    settings.license_file = license_file_arg;
  }
  if (symbol_map_file_arg) {
    settings.symbol_index_mapping_file = symbol_map_file_arg;
  }
  if(series_map_file_arg )
  {
    settings.series_index_mapping_file = series_map_file_arg;
  }



  // Initialise the feed.
  if (!(bpResult = feed->Init(settings))) {
    BP_LOG_ERROR() << "Failed to initialize " <<
      bp::feed::FeedNameToString(FEED_TYPE::kFeedName) <<
      " with "  << bpResult.ToString() << std::endl;
    return bpResult;
  }
  feed->SubscribeToAllFeedUpdates(callback, closure);



  // If we have a symbol file then subscribe to all of these directly
  bp::feed::xdp_v2::SymbolMappingReader smr;

  auto options_type = bp::feed::xdp_v2::ToOptionsFeedType(settings.feed_name);
  bool is_options_feed = bp::feed::xdp_v2::IsOptionsFeed(options_type);

  if (not settings.symbol_index_mapping_file.empty() and not is_options_feed) {
    if (not (bpResult = smr.InitFromFile(settings.symbol_index_mapping_file.c_str()))) {
      BP_LOGP_WARN("Received " << bpResult.ToString() << " when reading in " <<
                   settings.symbol_index_mapping_file << " in " <<
                   bp::feed::FeedNameToString(FEED_TYPE::kFeedName));
    }
  }
  if (not settings.series_index_mapping_file.empty() and is_options_feed) {
    bpResult = smr.InitOptionsFromFile(settings.series_index_mapping_file,
                                   settings.feed_name);
    if (not bpResult) {
      BP_LOGP_WARN("Received " << bpResult.ToString() << " when reading in " <<
                   settings.series_index_mapping_file << " in " <<
                   bp::feed::FeedNameToString(FEED_TYPE::kFeedName));
    }
  }



  // Subscribe to the products specified on the command line or otherwise those in the Symbol Mapping.
  if (products_arg.empty()) {
    std::vector<bp::core::Product> products = smr.GetAllSymbols();
    for(auto && product: products) {
      if (not (bpResult = feed->SubscribeToProduct(product, callback, closure))) {
        BP_LOGP_WARN("Received " << bpResult.ToString() << " when subscribing to " << product.name()
                    << " in " << bp::feed::FeedNameToString(FEED_TYPE::kFeedName));
      }
    }

    feed->SubscribeToAllProducts(callback, closure);
  } else {
    for (size_t i = 0; i < products_arg.size(); ++i) {
      if (!(bpResult = feed->SubscribeToProduct(bp::core::Product(products_arg[i]), callback, closure))) {
        BP_LOG_WARN() << "Received " << bpResult.ToString() << " when subscribing to " << products_arg[i]
          << " in " << bp::feed::FeedNameToString(FEED_TYPE::kFeedName) << std::endl;
      }
    }
  }



  // Print information about the feed sessions
  BP_LOG_DEBUG() << bp::feed::FeedNameToString(FEED_TYPE::kFeedName) << " Prev: Subscribed to " << feed->settings().sessions.size() << " sessions";

  feed->TrimUnusedProductSessions();
  BP_LOG_DEBUG() << bp::feed::FeedNameToString(FEED_TYPE::kFeedName) << " Trim: Subscribed to " << feed->settings().sessions.size() << " sessions";

  if (feed->sessions().size() == 0) {
    BP_LOG_ERROR() << "No sessions available";
    return bp::core::Result(bp::core::kResultCode_NoSessions);
  }



  // Register all the endpoints with the device
  for (size_t i = 0; i < feed->sessions().size(); ++i) {
    bp::feed::ISession* session = feed->sessions()[i];
    std::cout << "Adding Session: " << session->instance_name() << std::endl;
    for (size_t j = 0; j < session->endpoints().size(); ++j) {
      if (!(bpResult = pcap_device.RegisterIPEndpoint(session->endpoints()[j],
          &session->interface_addresses()[j],
          session)))
      {
        BP_LOG_ERROR() << bp::feed::FeedNameToString(FEED_TYPE::kFeedName)
          << ": Failed to register the endpoint " << session->endpoints()[j].ToString()
          << " over " << session->interface_addresses()[j].ToString()
          << " with error " << bpResult.ToString();
        return bpResult;
      }
    }
  }
  return bp::core::Result::Success();
}

void ex_program(int sig) {
  BP_UNUSED(sig);
  if (g_loop) {
    g_loop = false;
  } else {
    // upon second signal, just close
    exit(EXIT_FAILURE);
  }
}

int main(int argc, char* argv[]) {
  using bp::core::ProgramOptions;



  // Catch inturrupt signals (typically Ctrl-C on the command line) to allow a graceful shutdown.
  signal(SIGINT, ex_program);
  bp::sys::Socket::Init();



  // Process command line args
  bp::core::Result bpResult;
  if (!ProgramOptions::Init(argc, argv)) {
    std::cerr << "WARNING: Error processing arguments" << std::endl;
  }
  const char* license_file_arg           = ProgramOptions::GetSingleArgOption("--license-file");
  std::vector<std::string> pcaps_arg     = ProgramOptions::GetMultiArgOption("--pcaps", 1); // Mandatory
  const char* cfg_arg                    = ProgramOptions::GetSingleArgOption("--cfg");
  const char* feed_name_arg              = ProgramOptions::GetSingleArgOption("--feed-name");
  std::vector<std::string> products_arg  = ProgramOptions::GetMultiArgOption("--products");
  const char* symbol_map_file_arg        = ProgramOptions::GetSingleArgOption("--symbol-map-file");
  const char* series_map_file_arg        = ProgramOptions::GetSingleArgOption("--series-map-file");
  bool help_arg = ProgramOptions::IsFlagSet("-h") ||
                  ProgramOptions::IsFlagSet("--help");



  // Initial argument checks
  if (help_arg) {
    Usage();
    return EXIT_SUCCESS;
  }

  if (ProgramOptions::HasError()) {
    std::cerr << ProgramOptions::GetErrorString() << std::endl;
    Usage();
    return EXIT_FAILURE;
  }



  // Initialise the PCAP 'device' to read-in the PCAP file(s).
  bp::device::PcapMultiFileDevice pcap_device;
  bp::device::PcapMultiFileDeviceSettings pcap_device_settings;

  pcap_device_settings.verify_udp_checksum = false;
  pcap_device_settings.pcaps = pcaps_arg;

  if (!(bpResult = pcap_device.Init(pcap_device_settings))) {
    BP_LOG_ERROR() << "Init failed with " << bpResult.ToString() << std::endl;
    return EXIT_FAILURE;
  }



  // Use the feed specified in the arguments or default to `xdp_nyse_integrated`.
  std::string const feed_name = [&]() {
    if (feed_name_arg != NULL) {
      return std::string(feed_name_arg);
    } else {
      return bp::feed::xdp_v2::FeedSettings::xdp_nyse_integrated();
    }
  }();



  // Create a handler to be associated with the callback for various message types.
  TickerPrinter ticker_printer;
  bp::feed::IAggregatedPriceFeedCallback* callback = &ticker_printer;



  // Check if the xdp_v2 feed handler supports the specified feed; except for xdp_arca_book, which is not handled by _v2 at the moment.
  if (0 == strcmp("xdp_arca_book", feed_name.c_str())) {
    BP_LOGP_ERROR("xdp_arca_book is not supported by the bp::feed::xdp_v2 feed handler yet.  Use the bp::feed::xdp feed handler." << std::endl);
    return EXIT_FAILURE;
  }
  {
    auto const feed_names = bp::feed::xdp_v2::FeedSettings::FeedNames();
    auto iter = std::find(feed_names.begin(), feed_names.end(), feed_name);
    if (iter == feed_names.end()) {
      BP_LOGP_ERROR("Could not initialize feed: " << feed_name << ". Name not found in list of available feeds." << std::endl);
      BP_LOGP_ERROR("Please use one of the following feed names: " << GetFeedsAsString(", ") << "\n" << std::endl);
      Usage();
      return EXIT_FAILURE;
    }
  }



  // Create, configure and initialise a feed.
  bp::feed::xdp_v2::AggregatedPriceFeed aggregated_price_feed;

  bpResult = SetupFeed(
    feed_name.c_str(),
    cfg_arg,
    pcap_device,
    callback,
    (void*)feed_name.c_str() /*closure*/,
    products_arg,
    license_file_arg,
    symbol_map_file_arg,
    series_map_file_arg,
    &aggregated_price_feed);
  if (not bpResult)
  {
    BP_LOGP_ERROR("Init " << feed_name << " failed with " << bpResult.ToString());
    return EXIT_FAILURE;
  }



  // Start the device which is the source for the feed (i.e. open the PCAP files).
  printf("Starting %s\n", feed_name.c_str());
  printf("Press Control-C to exit\n");
  if (not (bpResult = pcap_device.Start())) {
    BP_LOG_ERROR() << "Failed to start the device with " << bpResult.ToString() << std::endl;
    return EXIT_FAILURE;
  }



  // Start processing messages from the feed.
  bp::sys::TimeSpan timeout = bp::sys::TimeSpan::FromWholeMilliseconds(1);
  bp::feed::ISession* session;
  bp::sys::Packet local_packet;
  bp::sys::Packet* packet = &local_packet;
  uint32_t packet_count = 0;

  std::string raw_command;
  while (g_loop) {
    bp::sys::Result res;
    if ((res = pcap_device.Fill(timeout, 1, &packet, &packet_count, (void**)&session))) {
      session->ProcessProtocol(1, packet);
    } else if (!res.Timeout() && !res.Interrupted()) {
      g_loop = false;
    }
  }



  // Loop finished, either by user cancellation, timeout or error/interruption on the underlying device.
  pcap_device.Stop();
  bp::core::Logging::Stop();

  return EXIT_SUCCESS;
}
