# frozen_string_literal: true

class Gouda::JobFuse < ActiveRecord::Base
  self.table_name = "gouda_job_fuses"
  self.primary_key = :active_job_class_name
end
