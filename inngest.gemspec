# frozen_string_literal: true

require_relative "lib/inngest/version"

Gem::Specification.new do |spec|
  spec.name = "inngest"
  spec.version = Inngest::VERSION
  spec.authors = ["Ben Wyrosdick"]
  spec.email = ["ben@vesyl.com"]

  spec.summary = "Ruby SDK for Inngest: durable functions, steps and workflows"
  spec.description = "Write durable, retriable functions with steps, sleeps and waits in Ruby and run them on " \
                     "Inngest over a persistent Connect worker."
  # TODO: set homepage/source_code_uri once the repo is public.
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.2"

  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "exe/*", "proto/*.proto", "LICENSE", "README.md"]
  spec.bindir = "exe"
  spec.executables = ["inngest-connect"]
  spec.require_paths = ["lib"]

  spec.add_dependency "google-protobuf", ">= 3.25", "< 5"
  spec.add_dependency "logger"
  spec.add_dependency "websocket-driver", ">= 0.7", "< 1"
end
