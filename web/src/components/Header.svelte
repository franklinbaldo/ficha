<script lang="ts">
	import { withBase } from '../lib/base';

	let { title = 'FICHA' } = $props();
	const homeHref = withBase('/');
	const consultHref = `${homeHref}#consulta`;
	const manifestHref = withBase('manifest.json');
</script>

<header data-cobogo-inverted>
	<nav aria-label="Navegação principal">
		<a class="brand" href={homeHref} aria-label="Ficha — início">
			<span class="wordmark">{title}</span>
			<span class="descriptor">fichário público de CNPJ</span>
		</a>
		<ul>
			<li><a href={consultHref}>Consultar</a></li>
			<li><a href={manifestHref}>Dados abertos</a></li>
			<li><a href={withBase('/sobre')}>Sobre</a></li>
		</ul>
	</nav>
</header>

<style>
	header {
		background: var(--cobogo-text);
		color: var(--cobogo-canvas);
		border-bottom: 6px solid var(--cobogo-accent-strong);
	}

	nav {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 2rem;
		max-width: 1180px;
		margin: 0 auto;
		padding: 1rem 2rem;
	}

	.brand {
		display: inline-flex;
		align-items: baseline;
		gap: 0.9rem;
		color: inherit;
		text-decoration: none;
	}

	.wordmark {
		font-family: var(--cobogo-font-display);
		font-size: 1.7rem;
		font-weight: 800;
		letter-spacing: -0.04em;
	}

	.descriptor {
		font-family: var(--cobogo-font-inscription);
		font-size: 0.66rem;
		font-weight: 700;
		letter-spacing: 0.12em;
		text-transform: uppercase;
		color: color-mix(in srgb, var(--cobogo-canvas) 68%, transparent);
	}

	ul {
		list-style: none;
		display: flex;
		gap: 1.4rem;
		margin: 0;
		padding: 0;
	}

	li {
		margin: 0;
	}

	a:not(.brand) {
		color: inherit;
		font-family: var(--cobogo-font-inscription);
		font-size: 0.72rem;
		font-weight: 800;
		letter-spacing: 0.08em;
		text-decoration: none;
		text-transform: uppercase;
		text-underline-offset: 0.35em;
	}

	a:not(.brand):hover,
	a:not(.brand):focus-visible {
		text-decoration: underline;
		text-decoration-color: var(--cobogo-accent-warm);
		text-decoration-thickness: 2px;
	}

	@media (max-width: 720px) {
		nav {
			align-items: flex-start;
			padding: 0.85rem 1.15rem;
		}

		.brand {
			align-items: flex-start;
			flex-direction: column;
			gap: 0.1rem;
		}

		.descriptor {
			display: none;
		}

		ul {
			gap: 0.85rem;
			flex-wrap: wrap;
			justify-content: flex-end;
		}

		a:not(.brand) {
			font-size: 0.65rem;
		}
	}

	@media (max-width: 430px) {
		ul li:nth-child(2) {
			display: none;
		}
	}
</style>
