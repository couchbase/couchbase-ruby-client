require "rspec"

RSpec.shared_examples "cluster-level queries" do
  describe "#query" do
    context "when performing a simple query" do
      let(:result) { cluster.query('SELECT "ruby rules" AS greeting') }

      it "the result has the correct number of rows" do
        expect(result.rows.size).to be 1
      end

      it "the result has the correct content" do
        expect(result.rows.first["greeting"]).to eq("ruby rules")
      end
    end

    context "when the query is badly formatted" do
      it "raises a ParsingFailure error" do
        expect { cluster.query('BAD QUERY') }.to raise_error(Couchbase::Error::ParsingFailure)
      end
    end

    context "with request_plus scan consistency" do
      let(:doc_id) { uniq_id(:foo) }
      let!(:cas) { collection.insert(doc_id, {"self_id" => doc_id}).cas }

      let(:result) do
        cluster.query(
          "SELECT META() AS meta FROM `#{bucket.name}` WHERE self_id = $doc_id",
          Couchbase::Options::Query(scan_consistency: :request_plus, named_parameters: {doc_id: doc_id})
        )
      end

      it "the result includes the document that was last inserted" do
        expect(result.rows.first["meta"]["id"]).to eq(doc_id)
      end

      it "the CAS of the row matches the CAS of the insert result" do
        expect(result.rows.first["meta"]["cas"]).to eq(cas)
      end
    end

    context "when performing a simple query with metrics enabled" do
      let(:metrics) do
        options = Couchbase::Options::Query.new(metrics: true, timeout: 200_000)
        result = cluster.query('SELECT "ruby_rules" AS greeting', options)
        result.meta_data.metrics
      end

      it { expect(metrics.elapsed_time).to be > 0 }
      it { expect(metrics.execution_time).to be > 0 }
      it { expect(metrics.error_count).to be 0 }
      it { expect(metrics.warning_count).to be 0 }
      it { expect(metrics.result_count).to be 1 }
    end

    context "when all the options are set" do
      let(:doc_id) do
        doc_id = uniq_id(:foo)
        collection.upsert(doc_id, {"foo" => "bar"})
        doc_id
      end

      let(:options) do
        Couchbase::Options::Query.new(
          adhoc: true,
          client_context_id: "123",
          max_parallelism: 3,
          metrics: true,
          pipeline_batch: 1,
          pipeline_cap: 1,
          readonly: true,
          scan_cap: 10,
          scan_consistency: :request_plus,
          scan_wait: 50,
          timeout: 200_000
        )
      end

      let(:result) do
        cluster.query("SELECT * FROM `#{bucket.name}` WHERE META().id = \"#{doc_id}\"", options)
      end

      it "the query's status is set to 'success'" do
        expect(result.meta_data.status).to be :success
      end

      it "the client context ID matches the one specified in the options" do
        expect(result.meta_data.client_context_id).to eq("123")
      end

      it "the metrics are returned" do
        expect(result.meta_data.metrics).not_to be_nil
      end
    end

    context "when the read-only condition is violated" do
      it "raises an InternalServerFailure error" do
        options = Couchbase::Options::Query.new(readonly: true)
        blk = proc { cluster.query("INSERT INTO `#{bucket.name}` (key, value) VALUES (\"foo\", \"bar\")", options) }
        expect(&blk).to raise_error(Couchbase::Error::InternalServerFailure)
      end
    end

    context "with a select query for one document" do
      let(:doc_id) do
        doc_id = uniq_id(:foo)
        collection.insert(doc_id, {"foo" => "bar"})
        doc_id
      end

      let(:result) do
        options = Couchbase::Options::Query.new(scan_consistency: :request_plus, timeout: 200_000)
        cluster.query("SELECT * FROM `#{bucket.name}` AS doc WHERE META().id = \"#{doc_id}\"", options)
      end

      it "the result has one row" do
        expect(result.rows.size).to be 1
      end

      it "the row has the correct content" do
        expect(result.rows.first["doc"]).to eq({"foo" => "bar"})
      end

      it "the metadata contains the request_id, client_context_id, and signature" do
        expect(result.meta_data).to have_attributes(
          request_id: lambda { |r| r.is_a?(String) },
          client_context_id: lambda { |c| c.is_a?(String) },
          signature: lambda { |s| s.is_a?(Hash) }
        )
      end

      it "there are no warnings" do
        expect(result.meta_data.warnings).to be_nil
      end
    end

    context "with a select query for multiple documents with sorting" do
      let(:doc_ids) do
        doc_ids = [:doc1, :doc2].map { |x| uniq_id(x) }

        doc_ids.each_with_index do |doc_id, idx|
          collection.insert(doc_id, {"foo" => "bar", "num" => idx})
        end
      end

      let(:result) do
        options = Couchbase::Options::Query.new(scan_consistency: :request_plus, timeout: 200_000)
        cluster.query(
          "SELECT * FROM `#{bucket.name}` AS doc WHERE META().id IN [\"#{doc_ids[0]}\", \"#{doc_ids[1]}\"] ORDER BY num",
          options
        )
      end

      it "the result has two rows" do
        expect(result.rows.size).to be 2
      end

      it "the rows have the correct content" do
        result.rows.each_with_index do |row, idx|
          expect(row["doc"]).to eq({"foo" => "bar", "num" => idx})
        end
      end
    end

    context "when the profile option is set" do
      context "with profiling disabled" do
        let(:result) do
          options = Couchbase::Options::Query.new(profile: :off, timeout: 200_000)
          cluster.query('SELECT "ruby rules" AS greeting', options)
        end

        it "the meta-data does not contain profiling information" do
          expect(result.meta_data.profile).to be_nil
        end
      end

      context "with the 'timings' profiling mode" do
        let(:result) do
          options = Couchbase::Options::Query.new(profile: :timings, timeout: 200_000)
          cluster.query('SELECT "ruby rules" AS greeting', options)
        end

        it "the meta-data contains profiling information that is represented with a Hash" do
          expect(result.meta_data.profile).to be_a(Hash)
        end

        it "the profiling information hash contains the 'executionTimings' key" do
          expect(result.meta_data.profile).to have_key("executionTimings")
        end
      end

      context "with the 'phases' profiling mode" do
        let(:result) do
          options = Couchbase::Options::Query.new(profile: :phases, timeout: 200_000)
          cluster.query('SELECT "ruby rules" AS greeting', options)
        end

        it "the meta-data contains profiling information that is represented with a Hash" do
          expect(result.meta_data.profile).to be_a(Hash)
        end

        it "the profiling information hash contains the 'phaseOperators' key" do
          expect(result.meta_data.profile).to have_key("phaseOperators")
        end
      end
    end
  end
end
