require 'json'
require 'colorize'
require 'flock/gather/comms'
require 'sparkr'

module Flock
  module Gather
    module Slurm
      module Wait
        class << self
          REF = Time.at(1526405203)

          def each_line(start_time, end_time, &block)
            case ENV['FLOCK_GATHER_MODE']
            when nil
              start_s = start_time.strftime("%Y-%m-%d-%H:%M")
              end_s = end_time.strftime("%Y-%m-%d-%H:%M")
              IO.popen(
                {'SLURM_TIME_FORMAT'=>'%s'},
                ['sacct','-p','-n',"-S#{start_s}","-E#{end_s}",'-X','-ojobid,submit,start,state']
              ) do |io|
                while !io.eof? do
                  block.call(io.readline)
                end
              end
            when 'fixture'
              IO.popen(['cat',"/tmp/sacct-data/sacct-data-#{start_time.to_i}-#{end_time.to_i}.txt"]) do |io|
                while !io.eof? do
                  block.call(io.readline)
                end
              end
            when 'generated'
              long = ((rand * 2).to_i) + 1
              (0..rand((end_time-start_time) / 100).to_i).each do |l|
                state = begin
                          i = (rand * 4).to_i
                          if i == 0
                            'COMPLETED'
                          elsif i == 1
                            'FAILED'
                          elsif i == 2
                            'CANCELLED by 12345'
                          else
                            'PENDING'
                          end
                        end
                submit_t = start_time + (rand * 7200).to_i
                start_t = submit_t + (rand * (86400/long)).to_i
                s = [
                  l.to_s,
                  submit_t.to_i,
                  start_t.to_i,
                  state,
                  "\n",
                ]
                block.call(s.join("|"))
              end
            end
          end

          def avg_queue(start_time, end_time)
            queue_times = []

            each_line(start_time, end_time) do |line|
              jobid, submit, start, state = line.split('|')
              submit = Time.at(submit.to_i)
              next if submit < start_time || submit >= end_time
              start = start == 'Unknown' ? REF : Time.at(start.to_i)
              queue_times << start - submit
            end

            total_queue = queue_times.reduce(:+)
            avg_queue = total_queue / queue_times.length
            avg_queue = avg_queue.to_i
            max_queue = queue_times.max
            {
              start_t: start_time,
              end_t: end_time,
              jobs: queue_times.length,
              mean: avg_queue.to_i,
              max: max_queue.to_i
            }
          end

          def median(array)
            sorted = array.sort
            len = sorted.length
            (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
          end

          def duration(d)
            d_s = d % 60
            d_m = (d % 3600) / 60
            d_h = (d % 86400) / 60 / 60
            d_d = d / 60 / 60 / 24
            [].tap do |parts|
              parts << "#{d_d}+" if d_d > 0
              parts << "#{sprintf '%02d', d_h}:" if d_d > 0 || d_h > 0
              parts << "#{sprintf '%02d', d_m}:"
              parts << "#{sprintf '%02d', d_s}"
            end.join('')
          end

          def transform(v)
            v
            #Math.log(v)
          end
          
          def spark_cell(val, data, key)
            vals = []
            days = []
            data.each do |a|
              day = a[:start_t].strftime('%a')[0]
              days <<
                if day == 'S'
                  day.cyan
                else
                  day.cyan.bold
                end
              vals << transform(a[key])
            end
            avg = (vals.reduce(:+) / vals.length)
            max = vals.max
            sparkline = Sparkr::Sparkline.new(vals)
            sparkline.format do |tick, count, index|
              if count == max
                tick.red
              elsif count > avg
                tick.yellow
              else
                tick.green
              end
            end

            (' ' * (val.to_s.length + 1)) <<
              days.join('') << "\n" <<
              val.to_s.cyan << ' ' <<
              sparkline.to_s
          end

          def colorize(val, base_val, data, key)
            vals = data.map{|d|d[key]}
            max = transform(vals.max)
            avg = vals.map(&method(:transform)).reduce(:+) / vals.length
            if transform(base_val) == max
              val.to_s.red
            elsif transform(base_val) > avg
              val.to_s.yellow
            else
              val.to_s.green
            end
          end

          def run
            t_arr = REF.to_a
            t_arr[2] = 0
            t_arr[1] = 0
            t_arr[0] = 0
            t = Time.local(*t_arr)

            rows = []
            data = (1..8).map do |n|
              avg_queue(t - 86400, t).tap do
                t = t - 86400
              end
            end

            queue_time = data.map {|d| d[:mean]}.reduce(:+)
            avg = (queue_time / data.length).to_i
            total_jobs = data.map{|d|d[:jobs]}.reduce(:+)
            max = data.map{|d|d[:max]}.max

            data.each do |d|
              row = []
              day = d[:start_t].strftime('%a')[0]
              row <<
                if day == 'S'
                  d[:start_t].strftime("%a %e").cyan
                else
                  d[:start_t].strftime("%a %e").cyan.bold
                end
              row << colorize(d[:jobs], d[:jobs], data, :jobs)
              row << colorize(duration(d[:mean]), d[:mean], data, :mean)
              row << colorize(duration(d[:max]), d[:max], data, :max)
              rows << row
            end
            rows << :separator
            rows << ['', total_jobs.to_s.cyan, duration(avg).cyan, duration(max).cyan]

            Flock::Gather::Comms.new(Gather.endpoint)
              .set('usage.week', rows.to_json, Gather.endpoint_auth)

            # table = Terminal::Table.new
            # table.headings = ['Day', 'Submissions', 'Wait (Mean)', 'Wait (Max)'].map{|s|s.bold}
            # table.title = "Weekly usage history".bold
            # table.rows = rows
            # puts table

            # table = Terminal::Table.new
            # table.headings = ['Cluster', 'Submissions', 'Wait (Mean)', 'Wait (Max)'].map{|s|s.bold}
            # table.title = "Cluster weekly usage".bold
            row = ["\nmarkt1"]
            row << spark_cell(total_jobs, data, :jobs)
            row << spark_cell(duration(avg), data, :mean)
            row << ("\n" << duration(max).cyan)
            #table.rows = [row]
            #puts table

            Flock::Gather::Comms.new(Gather.endpoint)
              .set('usage.week.submissions', row[1], Gather.endpoint_auth)
            Flock::Gather::Comms.new(Gather.endpoint)
              .set('usage.week.wait.mean', row[2], Gather.endpoint_auth)
            Flock::Gather::Comms.new(Gather.endpoint)
              .set('usage.week.wait.max', row[3], Gather.endpoint_auth)

            t_arr = REF.to_a
            t_arr[2] = (t_arr[2] / 4) * 4
            t_arr[1] = 0
            t_arr[0] = 0
            t = Time.local(*t_arr)
            rows = []
            data = (1..8).map do
              avg_queue(t - 14400, t).tap do |d|
                t = t - 14400
              end
            end

            queue_time = data.map {|d| d[:mean]}.reduce(:+)
            avg_avg = queue_time / data.length

            data.each do |d|
              row = []
              day = d[:start_t].strftime('%a')[0]
              row <<
                "#{d[:start_t].strftime("%a %H:%M")} - #{d[:end_t].strftime("%H:%M")}".cyan.bold
              row << colorize(d[:jobs], d[:jobs], data, :jobs)
              row << colorize(duration(d[:mean]), d[:mean], data, :mean)
              row << colorize(duration(d[:max]), d[:max], data, :max)
              rows << row
            end
            rows << :separator
            rows << ['',
                     data.map{|d|d[:jobs]}.reduce(:+).to_s.cyan,
                     duration(avg_avg.to_i).cyan,
                     duration(data.map{|d|d[:max]}.max).cyan]

            Flock::Gather::Comms.new(Gather.endpoint).
              set('usage.24hr', rows.to_json, Gather.endpoint_auth)

            # table = Terminal::Table.new
            # table.headings = ['Day', 'Submissions', 'Wait (Mean)', 'Wait (Max)'].map{|s| s.bold}
            # table.title = "24-hour usage history".bold
            # table.rows = rows
            # puts table
          end
        end
      end
    end
  end
end

Flock::Gather.register('slurm/wait', Flock::Gather::Slurm::Wait)
