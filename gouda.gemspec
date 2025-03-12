require_relative "lib/gouda/version"

Gem::Specification.new do |spec|
  spec.name = "gouda"
  spec.version = Gouda::VERSION
  spec.summary = "Job Scheduler"
  spec.description = "Job Scheduler for Rails and PostgreSQL"
  spec.authors = ["Sebastian van Hesteren", "Julik Tarkhanov"]
  spec.email = ["sebastian@cheddar.me", "me@julik.nl"]
  spec.homepage = "https://github.com/cheddar-me/gouda"
  spec.license = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 3.2.0")
  spec.require_paths = ["lib"]

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "https://github.com/cheddar-me/gouda/CHANGELOG.md"

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0")
  end

  rails_min = ">= 7.2.0"
  spec.add_dependency "activerecord", rails_min
  spec.add_dependency "activesupport", rails_min
  spec.add_dependency "railties", rails_min
  spec.add_dependency "activejob", rails_min
  spec.add_dependency "fugit", "~> 1.10"

  spec.add_development_dependency "standard"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "debug"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "appraisal"
end
