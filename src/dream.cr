require "sophia"

module Dream
  Sophia.define_env DreamEnv, {tags: {key: {tag: String,
                                            oid: String}}}

  class Index
    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {tags: opts}
    end

    def add(oid : String, tags : Array(String))
      @sophia << tags.map { |tag| {tag: tag, oid: oid} } unless @sophia.has_key?({tag: tags.first, oid: oid})
    end

    # looping (implemented):
    #      A          B          C
    #  1 (>=  0)  3 (>=  1) 10 (>=  3)
    # 11 (>= 10) 13 (>= 11) 14 (>= 13)
    # 14 (>= 14) 14 (>= 14) 14 (>= 14)
    # 17 (>  14) ...
    #
    # with backwards lookup:
    # A  B  C
    #  1
    #  1  3
    #  7  3
    #  7  7
    #  7  7 10
    # 11  7 10
    # 11 13 10
    # 14 13 10
    # 14 14 10
    # 14 14 14

    protected def find(tag : String)
      r = Set(String).new
      @sophia.from({tag: tag, oid: ""}) do |t|
        break unless t[:tag] == tag
        r << t[:oid]
      end
      r
    end

    getter next_count = 0_u64

    enum Strategy
      Intersect = 0
      Looping   = 1
    end

    def find(tags : Array(String), limit : UInt64 = UInt64::MAX, strategy : Strategy = Strategy::Looping)
      case strategy
      when Strategy::Intersect
        tags.map { |tag| find tag }.reduce { |acc, cur| acc &= cur }.to_a.sort[0..limit - 1]
      when Strategy::Looping
        r = [] of String

        cs = [] of DreamEnv::TagsCursor
        tags.each do |tag|
          cs << @sophia.cursor({tag: tag, oid: (cs.last.data.not_nil![:oid] rescue "")})
          return r unless (@next_count += 1) && cs.last.next
        end

        until r.size == limit
          r << cs.first.data.not_nil![:oid] if cs.all? { |c| c.data.not_nil![:oid] == cs.first.data.not_nil![:oid] }
          t = cs.first.data.not_nil![:tag]
          loop do
            return r unless (@next_count += 1) && cs.first.next && cs.first.data.not_nil![:tag] == t
            break if cs.first.data.not_nil![:oid] >= cs.last.data.not_nil![:oid]
          end
          cs.each_cons_pair do |c1, c2|
            t = c2.data.not_nil![:tag]
            until c2.data.not_nil![:oid] >= c1.data.not_nil![:oid]
              return r unless (@next_count += 1) && c2.next && c2.data.not_nil![:tag] == t
            end
          end
        end
        r
      else
        raise "unknown strategy #{strategy}"
      end
    end

    def clear
      @sophia.from({tag: "", oid: ""}) { |rec| @sophia.delete rec }
      @next_count = 0
    end
  end
end
