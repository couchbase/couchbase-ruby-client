require "rspec"

RSpec.shared_examples "a bucket" do
  describe "#default_collection" do
    let(:collection) { bucket.default_collection }

    it "returns the default collection" do
      expect(collection).to have_attributes(name: "_default", scope_name: "_default")
    end
  end

  describe "#default_scope" do
    let(:scope) { bucket.default_scope }

    it "returns the default scope" do
      expect(scope).to have_attributes(name: "_default")
    end
  end
end
