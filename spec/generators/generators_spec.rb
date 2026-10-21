# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require "rails/generators"
require "generators/inngest/install/install_generator"
require "generators/inngest/function/function_generator"

RSpec.describe "Inngest generators" do
  around do |example|
    Dir.mktmpdir do |dir|
      @root = File.join(dir, "my_shop")
      FileUtils.mkdir_p(File.join(@root, "config"))
      File.write(File.join(@root, "config/routes.rb"), "Rails.application.routes.draw do\nend\n")
      example.run
    end
  end

  def generate(generator, *args)
    capture_stdout { generator.start(args, destination_root: @root) }
  end

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end

  def read(path)
    File.read(File.join(@root, path))
  end

  def write(path, content)
    File.write(File.join(@root, path), content)
  end

  def valid_ruby?(code)
    RubyVM::InstructionSequence.compile(code)
    true
  rescue SyntaxError
    false
  end

  describe "inngest:install" do
    it "creates the initializer and functions directory" do
      generate(Inngest::Generators::InstallGenerator)

      initializer = read("config/initializers/inngest.rb")
      expect(initializer).to include('app_id: "my-shop"', "dev: Rails.env.development? || nil")
      expect(valid_ruby?(initializer)).to be(true)
      expect(File).to exist(File.join(@root, "app/inngest/.keep"))
    end

    it "adds the Connect worker to Procfiles, once" do
      write("Procfile", "web: bundle exec puma -C config/puma.rb")
      write("Procfile.dev", "web: bin/rails server\n")

      2.times { generate(Inngest::Generators::InstallGenerator) }

      expect(read("Procfile")).to eq("web: bundle exec puma -C config/puma.rb\ninngest: bundle exec inngest-connect\n")
      expect(read("Procfile.dev")).to eq(<<~PROCFILE)
        web: bin/rails server
        inngest: bundle exec inngest-connect
        inngest-dev: npx --yes inngest-cli@latest dev --no-discovery
      PROCFILE
      expect(read("config/routes.rb")).not_to include("Inngest::Serve")
    end

    it "mounts the endpoint in serve mode" do
      write("Procfile", "web: bundle exec puma\n")
      write("Procfile.dev", "web: bin/rails server\n")

      generate(Inngest::Generators::InstallGenerator, "--mode=serve")

      expect(read("config/routes.rb")).to include(%(mount Inngest::Serve.new => "/api/inngest"))
      expect(read("Procfile")).to eq("web: bundle exec puma\n")
      expect(read("Procfile.dev")).to include(
        "inngest-dev: npx --yes inngest-cli@latest dev -u http://localhost:3000/api/inngest --no-discovery"
      )
    end

    it "skips Procfiles that don't exist" do
      output = generate(Inngest::Generators::InstallGenerator)

      expect(File).not_to exist(File.join(@root, "Procfile"))
      expect(output).to include("bundle exec inngest-connect")
    end
  end

  describe "inngest:function" do
    it "creates a function triggered by an event named after it" do
      generate(Inngest::Generators::FunctionGenerator, "GeneratedWelcome")

      code = read("app/inngest/generated_welcome.rb")
      expect(code).to include("class GeneratedWelcome < Inngest::Function",
                              'trigger event: "app/generated-welcome"')

      load File.join(@root, "app/inngest/generated_welcome.rb")
      expect(GeneratedWelcome.id).to eq("generated-welcome")
      expect(GeneratedWelcome.triggers).to eq([{ "event" => "app/generated-welcome" }])
    end

    it "accepts event and cron triggers" do
      generate(Inngest::Generators::FunctionGenerator, "GeneratedReport", "--event=report/requested",
               "--cron=0 9 * * 1-5")

      load File.join(@root, "app/inngest/generated_report.rb")
      expect(GeneratedReport.triggers).to eq([{ "event" => "report/requested" }, { "cron" => "0 9 * * 1-5" }])
    end

    it "nests namespaced functions in modules" do
      generate(Inngest::Generators::FunctionGenerator, "generated_billing/send_invoice")

      code = read("app/inngest/generated_billing/send_invoice.rb")
      expect(code).to start_with(<<~RUBY)
        # frozen_string_literal: true

        module GeneratedBilling
          class SendInvoice < Inngest::Function
            trigger event: "generated-billing/send-invoice"
      RUBY
      expect(code).to end_with("    end\n  end\nend\n")

      load File.join(@root, "app/inngest/generated_billing/send_invoice.rb")
      expect(GeneratedBilling::SendInvoice.id).to eq("generated-billing-send-invoice")
    end

    it "generates a function that runs" do
      generate(Inngest::Generators::FunctionGenerator, "GeneratedRunner")
      load File.join(@root, "app/inngest/generated_runner.rb")

      _, body = execute(GeneratedRunner, event: { "name" => "app/generated-runner", "data" => { "x" => 1 } })
      expect(body.first).to include("op" => "StepRun", "data" => { "event_data" => { "x" => 1 } })
    end
  end
end
