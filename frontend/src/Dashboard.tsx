import { useEffect, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// Define strict TypeScript interfaces for API responses
interface ScoreBucket {
  bucket: string;
  count: number;
}

interface TimelineEntry {
  date: string;
  submissions: number;
}

interface PassRate {
  task: string;
  avg_score: number;
  attempts: number;
}

export function Dashboard() {
  const [labId, setLabId] = useState('lab-04');
  const [scores, setScores] = useState<ScoreBucket[]>([]);
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [passRates, setPassRates] = useState<PassRate[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);
      const token = localStorage.getItem('api_key');

      const headers = {
        Authorization: `Bearer ${token}`,
      };

      try {
            const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
            fetch(`/analytics/scores?lab=${labId}`, { headers }),
            fetch(`/analytics/timeline?lab=${labId}`, { headers }),
            fetch(`/analytics/pass-rates?lab=${labId}`, { headers }),
            ]);
        if (!scoresRes.ok || !timelineRes.ok || !passRatesRes.ok) {
          throw new Error('Failed to fetch analytics data. Did you run the sync pipeline?');
        }

        const scoresData: ScoreBucket[] = await scoresRes.json();
        const timelineData: TimelineEntry[] = await timelineRes.json();
        const passRatesData: PassRate[] = await passRatesRes.json();

        setScores(scoresData);
        setTimeline(timelineData);
        setPassRates(passRatesData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [labId]);

  // Chart Configurations
  const scoresChartData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
      },
    ],
  };

  const timelineChartData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions per Day',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
      },
    ],
  };

  return (
    <div className="dashboard-container">
      <div style={{ marginBottom: '20px' }}>
        <label htmlFor="lab-select" style={{ marginRight: '10px' }}>Select Lab: </label>
        <select
          id="lab-select"
          value={labId}
          onChange={(e) => setLabId(e.target.value)}
          style={{ padding: '5px' }}
        >
          <option value="lab-01">Lab 01</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-04">Lab 04</option>
          <option value="lab-05">Lab 05</option>
        </select>
      </div>

      {isLoading && <p>Loading dashboard data...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!isLoading && !error && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '30px' }}>
          
          {/* Bar Chart */}
          <div style={{ border: '1px solid #ccc', padding: '15px', borderRadius: '8px' }}>
            <h2>Score Distribution</h2>
            <Bar data={scoresChartData} />
          </div>

          {/* Line Chart */}
          <div style={{ border: '1px solid #ccc', padding: '15px', borderRadius: '8px' }}>
            <h2>Submission Timeline</h2>
            <Line data={timelineChartData} />
          </div>

          {/* Table */}
          <div style={{ gridColumn: '1 / -1', border: '1px solid #ccc', padding: '15px', borderRadius: '8px' }}>
            <h2>Pass Rates per Task</h2>
            <table style={{ width: '100%', textAlign: 'left' }}>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average Score</th>
                  <th>Total Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRates.map((pr) => (
                  <tr key={pr.task}>
                    <td>{pr.task}</td>
                    <td>{pr.avg_score}%</td>
                    <td>{pr.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
        </div>
      )}
    </div>
  );
}