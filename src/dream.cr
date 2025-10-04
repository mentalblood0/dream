require "yaml"

require "sophia"
require "xxhash128"

module Dream
  Sophia.define_env Env, {t2o: {key: {t2ot0: UInt64,
                                      t2ot1: UInt64,
                                      t2oo0: UInt64,
                                      t2oo1: UInt64}},
                          o2t: {key: {o2to0: UInt64,
                                      o2to1: UInt64,
                                      o2tt0: UInt64,
                                      o2tt1: UInt64}},
                          d2v: {key: {d2vd0: UInt64,
                                      d2vd1: UInt64},
                                value: {d2vv: Bytes}},
                          c: {key: {ti0: UInt64,
                                    ti1: UInt64},
                              value: {c: UInt64}}}

  alias Id = {UInt64, UInt64}

  struct T2o
    getter tup : {t2ot0: UInt64, t2ot1: UInt64, t2oo0: UInt64, t2oo1: UInt64}

    def initialize(@tup)
    end

    def initialize(t : Id, o : Id)
      @tup = {t2ot0: t[0], t2ot1: t[1], t2oo0: o[0], t2oo1: o[1]}
    end

    def t
      {@tup[:t2ot0], @tup[:t2ot1]}
    end

    def o
      {@tup[:t2oo0], @tup[:t2oo1]}
    end

    def >=(another : T2o)
      (@tup[:t2oo0] >= another.tup[:t2oo0]) ||
        ((@tup[:t2oo0] == another.tup[:t2oo0]) && (@tup[:t2oo1] >= another.tup[:t2oo1]))
    end
  end

  class Index
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Env

    @[YAML::Field(ignore: true)]
    property intx = false

    def initialize(@env : Env)
    end

    def transaction(&)
      if @intx
        yield self
      else
        @env.transaction do |tx|
          r = Index.new tx
          r.intx = true
          yield r
        end
      end
    end

    protected def digest(s : Bytes)
      d = LibXxhash.xxhash128 s, s.size, 0
      {d.high64, d.low64}
    end

    protected def memoize(i : Id, v : Bytes)
      @env << {d2vd0: i[0], d2vd1: i[1], d2vv: v} unless @env[{d2vd0: i[0], d2vd1: i[1]}]?
    end

    protected def forget(i : Id)
      @env.delete({d2vd0: i[0], d2vd1: i[1]})
    end

    def add(o : Bytes | Id, ts : Array(Bytes | Id))
      oi = (o.is_a? Bytes) ? (digest o) : o
      transaction do |tx|
        tx.memoize oi, o if o.is_a? Bytes
        ts.each do |t|
          ti = (t.is_a? Bytes) ? (digest t) : t
          tx.memoize ti, t if t.is_a? Bytes
          tx.env << {t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]}
          tx.env << {o2to0: oi[0], o2to1: oi[1], o2tt0: ti[0], o2tt1: ti[1]}
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] rescue 0_u64) + 1}
        end
      end
    end

    def []?(i : Id)
      @env[{d2vd0: i[0], d2vd1: i[1]}]?.not_nil![:d2vv].clone rescue nil
    end

    def get(o : Bytes | Id, &)
      oi = (o.is_a? Bytes) ? (digest o) : o
      @env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
        break unless {o2t[:o2to0], o2t[:o2to1]} == oi
        yield({o2t[:o2tt0], o2t[:o2tt1]})
      end
    end

    def get(o : Bytes | Id) : Array(Id)
      r = [] of Id
      get(o) { |t| r << t }
      r
    end

    def delete(o : Bytes | Id)
      oi = (o.is_a? Bytes) ? (digest o) : o
      transaction do |tx|
        @env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
          break unless {o2t[:o2to0], o2t[:o2to1]} == oi
          ti = {o2t[:o2tt0], o2t[:o2tt1]}
          tx.env.delete({t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]})
          tx.env.delete({o2to0: ti[0], o2to1: ti[1], o2tt0: oi[0], o2tt1: oi[1]})
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] - 1 rescue 0_u64)}
        end
        tx.forget oi if o.is_a? Bytes
      end
    end

    def delete(o : Bytes | Id, ts : Array(Bytes | Id))
      oi = (o.is_a? Bytes) ? (digest o) : o
      transaction do |tx|
        ts.each do |t|
          ti = (t.is_a? Bytes) ? (digest t) : t
          tx.env.delete({t2ot0: ti[0], t2ot1: ti[1], t2oo0: oi[0], t2oo1: oi[1]})
          tx.env.delete({o2to0: oi[0], o2to1: oi[1], o2tt0: ti[0], o2tt1: ti[1]})
          tx.env << {ti0: ti[0], ti1: ti[1], c: (@env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] - 1 rescue 0_u64)}
        end
      end
      transaction do |tx|
        tx.env.from({o2to0: oi[0], o2to1: oi[1], o2tt0: 0_u64, o2tt1: 0_u64}) do |o2t|
          if {o2t[:o2to0], o2t[:o2to1]} == oi
            return
          else
            break
          end
        end
        tx.forget oi if o.is_a? Bytes
      end
    end

    def find(present : Array(Bytes | Id), absent : Array(Bytes | Id) = [] of Bytes | Id, from : Id? = nil, &)
      ais = absent.compact_map { |t| (t.is_a? Bytes) ? (digest t) : t }
      ais.sort_by! { |ti| @env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] rescue UInt64::MAX }
      ais.reverse!

      pis = present.map { |t| (t.is_a? Bytes) ? (digest t) : t }
      pis.sort_by! { |ti| @env[{ti0: ti[0], ti1: ti[1]}]?.not_nil![:c] rescue return }

      if pis.size == 1
        ti = pis.first
        @env.from((T2o.new ti, (from ? from : {0_u64, 0_u64})).tup, from ? ">" : ">=") do |t2o|
          break if {t2o[:t2ot0], t2o[:t2ot1]} != ti
          yield({t2o[:t2oo0], t2o[:t2oo1]}) if ais.all? { |ai| !@env.has_key? (T2o.new ai, (T2o.new t2o).o).tup }
        end
        return
      end

      cs = [] of Env::T2oCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| (T2o.new c.data.not_nil!).o == (T2o.new cs.first.data.not_nil!).o }
          if ais.all? { |ai| !@env.has_key? (T2o.new ai, (T2o.new cs.first.data.not_nil!).o).tup }
            yield({cs.first.data.not_nil![:t2oo0], cs.first.data.not_nil![:t2oo1]})
          end
          return unless cs.first.next && ((T2o.new cs.first.data.not_nil!).t == pis.first)
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @env.cursor((T2o.new pis[i1], (from ? from : {0_u64, 0_u64})).tup, from ? ">" : ">=")
          else
            c = @env.cursor((T2o.new pis[i1], (T2o.new cs.last.data.not_nil!).o).tup)
          end
          return unless c.next && ((T2o.new c.data.not_nil!).t == pis[i1])
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @env.cursor((T2o.new pis[i2], (T2o.new cs.last.data.not_nil!).o).tup)
          return unless c.next && ((T2o.new c.data.not_nil!).t == pis[i2])
          cs << c
        end
        c2 = cs[i2]

        until (T2o.new c2.data.not_nil!).o >= (T2o.new c1.data.not_nil!).o
          return unless c2.next && ((T2o.new c2.data.not_nil!).t == pis[i2])
        end
        if ((T2o.new c2.data.not_nil!).o == (T2o.new c1.data.not_nil!).o)
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until (T2o.new cs.first.data.not_nil!).o >= (T2o.new cs[i2].data.not_nil!).o
            return unless cs.first.next && ((T2o.new cs.first.data.not_nil!).t == pis.first)
          end
          i1 = 0
          i2 = 1
        end
      end
    end

    def find(present : Array(Bytes | Id), absent : Array(Bytes | Id) = [] of Bytes, limit : UInt64 = UInt64::MAX, from : Id? = nil) : Array(Id)
      r = [] of Id
      find(present, absent, from) do |o|
        break if r.size == limit
        r << o
      end
      r
    end
  end
end
