require_relative "lib/gouda/version"

Gem::Specification.new do |spec|
  spec.name = "gouda"
  spec.version = Gouda::VERSION
  spec.summary = "Job Scheduler"
  spec.description = "Job Scheduler for Rails and PostgreSQL"
  spec.authors = ["Sebastian van Hesteren", "Julik Tarkhanov"]
  spec.email = ["sebastian@cheddar.me", "me@julik.nl"]
  spec.homepage = "https://rubygems.org/gems/gouda"
  spec.license = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.7.0")
  spec.require_paths = ["lib"]

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/cheddar-me/gouda"
  spec.metadata["changelog_uri"] = "https://github.com/cheddar-me/gouda/CHANGELOG.md"

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0")
  end

  spec.add_dependency "activerecord", "~> 7"
  spec.add_dependency "activesupport", "~> 7"
  spec.add_dependency "railties", "~> 7"
  spec.add_dependency "activejob", "~> 7"
  spec.add_dependency "fugit", "~> 1.10.1"

  spec.add_development_dependency "pg"
  spec.add_development_dependency "debug"
  spec.add_development_dependency "pry"
end
