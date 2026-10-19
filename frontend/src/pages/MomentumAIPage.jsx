import PageHeader from '../components/PageHeader'
import MomentumAnalysis from '../components/MomentumAnalysis'

export default function MomentumAIPage() {
  return <div className="page-stack">
    <PageHeader title="Momentum · IA" subtitle="Analyse globale de l’environnement et axes d’amélioration de la stratégie momentum" />
    <MomentumAnalysis />
  </div>
}
