require "sophia"

module Dream
  Sophia.define_env Env, {ii: {key: {ti: UInt32,
                                     oi: UInt32}},
                          i2t: {key: {i2ti: UInt32},
                                value: {i2tt: String}},
                          t2i: {key: {t2it: String},
                                value: {t2ii: UInt32}},
                          i2o: {key: {i2oi: UInt32},
                                value: {i2oo: String}},
                          o2i: {key: {o2io: String},
                                value: {o2ii: UInt32}},
                          c: {key: {ti: UInt32},
                              value: {c: UInt32}}}

  class Index
    @tc : UInt32
    @oc : UInt32

    def initialize(@sophia : Env)
      @tc = (@sophia.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] rescue 0_u32) + 1
      @oc = (@sophia.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] rescue 0_u32) + 1
    end

    def tags_count
      @tc - 1
    end

    def objects_count
      @oc - 1
    end

    def add(object : String, tags : Array(String))
      @sophia.transaction do |tx|
        oi = (@sophia[{o2io: object}]?.not_nil![:o2ii] rescue begin
          tx << {o2io: object, o2ii: @oc}
          tx << {i2oi: @oc, i2oo: object}
          @oc += 1
          @oc - 1
        end)
        tags.each do |tag|
          ti = (@sophia[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx << {t2it: tag, t2ii: @tc}
            tx << {i2ti: @tc, i2tt: tag}
            @tc += 1
            @tc - 1
          end)
          tx << {ti: ti, oi: oi}
          tx << {ti: ti, c: (@sophia[{ti: ti}]?.not_nil![:c] rescue 0_u32) + 1}
        end
      end
    end

    def find(present : Array(String), absent : Array(String) = [] of String, limit : UInt32 = UInt32::MAX, from : String? = nil)
      fromi = if from
                @sophia[{o2io: from}]?.not_nil![:o2ii]
              else
                nil
              end

      ais = absent.compact_map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue nil }
      ais.sort_by! { |ti| @sophia[{ti: ti}]?.not_nil![:c] }
      ais.reverse!

      r = [] of String
      if present.size == 1
        ti = @sophia[{t2it: present.first}]?.not_nil![:t2ii] rescue return r
        @sophia.from({ti: ti, oi: (fromi.not_nil! rescue 0_u32)}, ">") do |ii|
          break if r.size == limit || ii[:ti] != ti
          r << @sophia[{i2oi: ii[:oi]}]?.not_nil![:i2oo] if ais.all? { |ai| !@sophia.has_key?({ti: ai, oi: ii[:oi]}) }
        end
        return r
      end

      pis = present.map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue return r }
      pis.sort_by! { |ti| @sophia[{ti: ti}]?.not_nil![:c] }

      cs = [] of Dream::Env::IiCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| c.data.not_nil![:oi] == cs.first.data.not_nil![:oi] }
          if ais.all? { |ai| !@sophia.has_key?({ti: ai, oi: cs.first.data.not_nil![:oi]}) }
            r << @sophia[{i2oi: cs.first.data.not_nil![:oi]}]?.not_nil![:i2oo]
            return r if r.size == limit
          end
          return r unless cs.first.next && cs.first.data.not_nil![:ti] == pis.first
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @sophia.cursor({ti: pis[i1], oi: (fromi.not_nil! rescue 0_u32)}, ">")
          else
            c = @sophia.cursor({ti: pis[i1], oi: cs.last.data.not_nil![:oi]})
          end
          return r unless c.next && c.data.not_nil![:ti] == pis[i1]
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @sophia.cursor({ti: pis[i2], oi: cs.last.data.not_nil![:oi]})
          return r unless c.next && c.data.not_nil![:ti] == pis[i2]
          cs << c
        end
        c2 = cs[i2]

        until c2.data.not_nil![:oi] >= c1.data.not_nil![:oi]
          return r unless c2.next && c2.data.not_nil![:ti] == pis[i2]
        end
        if c2.data.not_nil![:oi] == c1.data.not_nil![:oi]
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until cs.first.data.not_nil![:oi] >= cs[i2].data.not_nil![:oi]
            return r unless cs.first.next && cs.first.data.not_nil![:ti] == pis.first
          end
          i1 = 0
          i2 = 1
        end
      end
    end
  end
end
