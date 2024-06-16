# frozen_string_literal: true

# The sole purpose of this class is so that you can do
# `config.active_job.queue_adapter = :gouda` in your Rails
# config, as Rails insists on resolving the adapter module
# name from the symbol automatically. If Rails ever allows
# us to "register" an adapter to a symbol this module can
# be removed later.
module ActiveJob::QueueAdapters::GoudaAdapter < Gouda::Adapter
end
