# Frontend Cache Revalidation API

Cette fonctionnalité permet de notifier automatiquement votre frontend quand de nouveaux résultats sont sauvegardés, forçant la revalidation du cache.

## Configuration

Ajoutez le flag `--revalidation-api` avec l'URL de votre API de revalidation :

```bash
./google-maps-scraper --dsn "postgres://user:pass@localhost/db" --revalidation-api "https://leadexpress.nexum.services/api/results/revalidate"
```

## API Frontend

Votre API de revalidation doit accepter des requêtes POST avec le format suivant :

```json
{
  "userId": "user123"
}
```

Exemple d'implémentation Next.js :

```typescript
import { resetQueue } from "@/components/results/cache-results";
import { NextResponse } from "next/server";

export async function POST(request: Request) {
  try {
    const { userId } = await request.json();

    if (!userId) {
      return NextResponse.json(
        { error: "userId is required" },
        { status: 400 }
      );
    }

    await resetQueue(true, userId);
    return NextResponse.json({ message: "Revalidated", userId });
  } catch (error) {
    return NextResponse.json(
      { error: "Failed to revalidate" },
      { status: 500 }
    );
  }
}
```

## Fonctionnement

1. Quand des résultats sont sauvegardés en base de données
2. Le système extrait les `userId` uniques des résultats
3. Pour chaque `userId` unique, un appel POST asynchrone est fait vers votre API
4. Votre frontend peut alors revalider le cache pour cet utilisateur

## Gestion d'erreurs

- Les erreurs HTTP n'affectent pas la sauvegarde des résultats
- Les appels sont faits de manière asynchrone (goroutines)
- Timeout de 10 secondes par requête
- Si l'URL n'est pas configurée, aucune requête n'est envoyée









