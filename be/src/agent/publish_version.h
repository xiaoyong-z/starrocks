// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_set>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class ThreadPoolToken;
class DataDir;

void run_publish_version_task(ThreadPoolToken* token, const TAgentTaskRequest& publish_version_task,
                              TFinishTaskRequest& finish_task, std::unordered_set<DataDir*>& affected_dirs);

} // namespace starrocks
