# Beagle's Basket Realtime Worker

This Worker creates the `BasketRoom` Durable Object used by Beagle's Basket for live shared-list sync.

Deploy command:

```bash
npx wrangler deploy
```

After deployment, bind the Durable Object namespace to the Beagle's Basket Pages project:

- Variable name: `BASKET_ROOM`
- Durable Object namespace: the `BasketRoom` namespace from this Worker
